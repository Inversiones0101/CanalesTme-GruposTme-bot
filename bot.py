#!/usr/bin/env python3
"""
Bot de Canales Financieros - MODO LIGERO v2.3
Mejoras aplicadas:
  - Hash basado en texto completo (evita colisiones entre mensajes similares)
  - MAX_MENSAJES_POR_CANAL subido a 5 (no pierde mensajes si el bot falla una corrida)
  - allow_redirects=True en requests (algunos canales redireccionan)
  - Logs detallados cuando el extractor devuelve 0 mensajes
  - _limpiar_formato_mixto simplificado y más seguro
"""
import os
import sys
import re
import json
import hashlib
import time
from datetime import datetime

# ============= CONFIGURACIÓN =============
MAX_MENSAJES_POR_CANAL = 5          # ← MEJORA: era 3, subido a 5
DELAY_ENTRE_MENSAJES = 2.5
TIMEOUT_REQUEST = 10

CANALES_IMAGENES_PUBLICAS = [
    "@ravabursatil",
    "@canalareadeinversores",
    "@ambitoArg",
    "@gruposbs",
    "@iolinvertironline",
    "@BalanzCapital",
    "@marketbostonam",
    "@PortfolioPersonalInversiones",
]

BLACKLIST_GLOBAL = [
    "Monitor de Precios", "Monitor de Bonos", "Resumen de Precios",
    "AperturaDeMercado", "Apertura de mercado", "Cierre de mercado",
    "CierreDeMercado", "PYME", "PyME", "Mercados en vivo", "🚦"
]

CONFIG_CANALES = {
    "@PortfolioPersonalInversiones": {
        "nombre": "📈 PPI",
        "keywords": ["Informe", "Daily Mercados", "Perspectivas de la semana", "Trading Comment"],
        "emoji": "📋",
        "imagenes_publicas": True
    },
    "@gruposbs": {
        "nombre": "☕ Grupo SBS",
        "keywords": ["Buenos días", "Mercado", "SBSQuickNote", "Resumen de la rueda"],
        "emoji": "📰",
        "imagenes_publicas": True
    },
    "@ambitoArg": {
        "nombre": "🅰️ Ámbito",
        "keywords": ["la city"],
        "emoji": "🏦",
        "imagenes_publicas": True
    },
    "@canalareadeinversores": {
        "nombre": "🎯 Área de Inversores",
        "keywords": ["lo que hay que saber esta mañana", "saber esta mañana", "Fuente BCR Mercados",
                     "La Secretaría de Finanzas anuncia", "una nueva licitación", "licitación del día de hoy"],
        "emoji": "🌅",
        "imagenes_publicas": True
    },
    "@ravabursatil": {
        "nombre": "📉 Rava Bursátil",
        "keywords": ["Rava.com/vivo"],
        "emoji": "📊",
        "imagenes_publicas": True
    },
    "@SoloBONOS": {
        "nombre": "📄 ON y Bonos",
        "keywords": ["ON", "Clase", "Serie", "pago", "vencimiento"],
        "emoji": "🏛️"
    },
    "@iolinvertironline": {
        "nombre": "🏦 IOL",
        "keywords": ["Nueva licitación", "Nueva licitación de Letras", "Nueva licitación de Letras y Bonos", "Portafolio"],
        "emoji": "💰",
        "imagenes_publicas": True
    },
    "@BalanzCapital": {
        "nombre": "⚖️ Balanz Capital",
        "keywords": ["Fondos Destacados del Mes", "Nueva licitación del Tesoro", "Agenda de la semana"],
        "emoji": "🎯",
        "imagenes_publicas": True
    },
    "@marketbostonam": {
        "nombre": "🗽 Boston AM",
        "keywords": ["TRADE IDEA", "CEDEAR del día", "Acción del día", "Nuevo informe de opciones", "opciones"],
        "emoji": "⚡",
        "imagenes_publicas": True
    }
}

# ============= LOGGER =============
class Logger:
    @staticmethod
    def info(msg): print(f"ℹ️  {msg}")
    @staticmethod
    def ok(msg): print(f"✅ {msg}")
    @staticmethod
    def warn(msg): print(f"⚠️  {msg}")
    @staticmethod
    def error(msg): print(f"❌ {msg}")

# ============= FORMATEO =============
def formatear_texto(texto, canal_config):
    if not texto:
        return ""
    
    reemplazos = {
        '&lt;': '<', '&gt;': '>', '&amp;': '&',
        '&quot;': '"', '&#39;': "'", '&#33;': '!',
        '&#036;': '$', '&#8211;': '–', '&nbsp;': ' '
    }
    for viejo, nuevo in reemplazos.items():
        texto = texto.replace(viejo, nuevo)
    
    texto = texto.replace('\r\n', '\n').replace('\r', '\n')
    while '\n\n\n' in texto:
        texto = texto.replace('\n\n\n', '\n\n')
    
    def resaltar_ticker(match):
        ticker = match.group(1) or match.group(2)
        return f"💠 <code>{ticker}</code>"
    
    texto = re.sub(r'\$([A-Z]{1,5})\b|(?<!\w)([A-Z]{2,5})(?=\s*(?:gráfico|grafico|NY|NYSE|NASDAQ|:))',
                   resaltar_ticker, texto)
    
    texto = re.sub(r'(\$\d[\d,.]*(?:usd|ARS)?)\b', r'<b>\1</b>', texto, flags=re.IGNORECASE)
    
    palabras_clave = {
        r'\b(objetivo|target|stop loss|resistencia|soporte)\b': '🎯',
        r'\b(compra|bullish|alcista|sube)\b': '🟢',
        r'\b(venta|bearish|bajista|baja)\b': '🔴',
        r'\b(neutro|hold|esperar)\b': '⚪',
    }
    
    for patron, emoji in palabras_clave.items():
        texto = re.sub(patron, f"{emoji} <b>\\1</b>", texto, flags=re.IGNORECASE)
    
    lineas = [line.strip() for line in texto.split('\n')]
    texto = '\n'.join(lineas)
    
    return texto.strip()

def crear_header(canal_config):
    emoji = canal_config.get('emoji', '📡')
    nombre = canal_config['nombre']
    return f"{emoji} <b>{nombre}</b>\n{'─' * 20}\n\n"

def crear_footer(tiene_imagen=False, url_imagen=None):
    if tiene_imagen and url_imagen:
        return f"\n\n📎 <a href='{url_imagen}'>Ver gráfico</a>"
    return ""

# ============= TELEGRAM =============
class BotTelegram:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        if not self.token or not self.chat_id:
            Logger.error("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")
            sys.exit(1)

    def enviar_mensaje(self, texto, parse_mode='HTML'):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        
        try:
            import requests
            payload = {
                'chat_id': self.chat_id,
                'text': texto[:4000],
                'parse_mode': parse_mode,
                'disable_web_page_preview': False,
                'disable_notification': True
            }
            
            resp = requests.post(url, json=payload, timeout=TIMEOUT_REQUEST)
            data = resp.json()
            
            if not data.get('ok'):
                # Si falla por HTML mal formado, reintenta sin formato
                if "parse" in str(data.get('description', '')).lower():
                    Logger.warn("HTML inválido, reintentando sin formato...")
                    payload['parse_mode'] = None
                    payload['text'] = re.sub(r'<[^>]+>', '', texto)[:4000]
                    resp = requests.post(url, json=payload, timeout=TIMEOUT_REQUEST)
                    return resp.json().get('ok', False)
                Logger.error(f"Telegram rechazó mensaje: {data.get('description', '')}")
                return False
            
            return True
            
        except Exception as e:
            Logger.error(f"Error enviando: {e}")
            return False

# ============= EXTRACTOR =============
class ExtractorTelegramWeb:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9',
            'Cache-Control': 'no-cache',
        }

    def obtener_mensajes(self, username):
        url = f"https://t.me/s/{username.replace('@', '')}"
        
        try:
            import requests
            # ← MEJORA: allow_redirects=True para canales que redireccionan
            resp = requests.get(url, headers=self.headers, timeout=TIMEOUT_REQUEST, allow_redirects=True)
            
            if resp.status_code != 200:
                # ← MEJORA: log detallado cuando falla el scraping
                Logger.error(f"HTTP {resp.status_code} en {username} — URL final: {resp.url}")
                return []
            
            patron = r'<div class="tgme_widget_message[^"]*".*?<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>.*?(?:<a class="tgme_widget_message_photo_wrap[^"]*".*?href="([^"]*)")?.*?</div>\s*</div>'
            
            mensajes = []
            for match in re.finditer(patron, resp.text, re.DOTALL):
                texto_html = match.group(1)
                link_imagen = match.group(2)
                
                texto = self._limpiar_html(texto_html)
                if len(texto) > 10:
                    # ← MEJORA CRÍTICA: hash sobre texto completo, no solo 100 chars
                    # Esto evita colisiones entre mensajes que empiezan igual
                    # (ej: dos "Resumen de la rueda de hoy 📅..." del mismo canal)
                    id_hash = hashlib.md5(texto.encode()).hexdigest()[:16]
                    mensajes.append({
                        'texto': texto,
                        'imagen': link_imagen,
                        'id_hash': id_hash
                    })
            
            # ← MEJORA: log si el extractor no encontró nada (ayuda a detectar cambios en el HTML de Telegram)
            if not mensajes:
                Logger.warn(f"Extractor devolvió 0 mensajes para {username} — puede que Telegram cambió su HTML o el canal es privado")
            else:
                Logger.info(f"Extraídos {len(mensajes)} mensajes de {username}")

            return mensajes[-MAX_MENSAJES_POR_CANAL:]
            
        except Exception as e:
            Logger.error(f"Error extrayendo {username}: {e}")
            return []

    def _limpiar_html(self, html):
        texto = html.replace('<br/>', '\n').replace('<br>', '\n')
        texto = re.sub(r'</p>\s*<p>', '\n\n', texto)
        texto = re.sub(r'<[^>]+>', '', texto)
        texto = texto.replace('&nbsp;', ' ').replace('&quot;', '"')
        return texto.strip()

# ============= HISTORIAL =============
class GestorHistorial:
    def __init__(self, archivo='ultimo_id_canales.json'):
        self.archivo = archivo
        self.data = self._cargar()

    def _cargar(self):
        try:
            if not os.path.exists(self.archivo):
                Logger.info("📄 Creando nuevo archivo de historial")
                return {}
            
            with open(self.archivo, 'r', encoding='utf-8') as f:
                contenido = f.read().strip()
                
                if not contenido:
                    Logger.warn("📄 Historial vacío, creando nuevo")
                    return {}
                
                data = json.loads(contenido)
                return self._limpiar_formato_mixto(data)
                
        except json.JSONDecodeError as e:
            Logger.error(f"JSON corrupto: {e}. Creando nuevo historial.")
            try:
                os.rename(self.archivo, f"{self.archivo}.backup")
                Logger.warn(f"Backup guardado en {self.archivo}.backup")
            except:
                pass
            return {}
        except Exception as e:
            Logger.error(f"Error cargando historial: {e}")
            return {}

    def _limpiar_formato_mixto(self, data):
        """
        MEJORA: lógica simplificada y más segura.
        El formato viejo guardaba IDs como '@canal_hashXXXX'.
        El nuevo formato guarda solo el hash 'hashXXXX'.
        Esta función normaliza ambos al nuevo formato.
        """
        limpio = {}
        for canal, ids in data.items():
            ids_limpios = []
            for id_item in ids:
                if isinstance(id_item, str) and id_item.startswith('@') and '_' in id_item:
                    # Formato viejo: '@canal_hashXXXX' → extraer solo 'hashXXXX'
                    # Busca el primer '_' después del @ y toma todo lo que sigue
                    partes = id_item.split('_', 1)
                    if len(partes) == 2 and len(partes[1]) == 16:
                        # Solo migrar si la parte después de _ parece un hash válido (16 chars hex)
                        ids_limpios.append(partes[1])
                    else:
                        # Si no tiene el formato esperado, guardar tal cual
                        ids_limpios.append(id_item)
                else:
                    ids_limpios.append(id_item)
            
            # Deduplicar y mantener los últimos 30
            limpio[canal] = list(dict.fromkeys(ids_limpios))[-30:]
        
        return limpio

    def es_nuevo(self, canal, msg_id):
        if canal not in self.data:
            self.data[canal] = []
        
        if msg_id in self.data[canal]:
            return False
        
        self.data[canal].append(msg_id)
        self.data[canal] = self.data[canal][-30:]
        return True

    def guardar(self):
        try:
            with open(self.archivo, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=1, ensure_ascii=False)
            Logger.ok("💾 Historial guardado")
        except Exception as e:
            Logger.error(f"Error guardando historial: {e}")

# ============= FILTROS =============
class FiltroContenido:
    @staticmethod
    def pasa_blacklist(texto):
        texto_lower = texto.lower()
        return not any(frase.lower() in texto_lower for frase in BLACKLIST_GLOBAL)
    
    @staticmethod
    def tiene_keywords(texto, keywords):
        if not keywords:
            return True
        
        texto_lower = texto.lower()
        
        for kw in keywords:
            kw_lower = kw.lower()
            kw_sin_simbolos = re.sub(r'[^\w]', '', kw_lower)
            
            if kw_lower in texto_lower:
                return True
            if kw_sin_simbolos and kw_sin_simbolos in texto_lower:
                return True
            if re.search(r'\b' + re.escape(kw_sin_simbolos) + r'\b', texto_lower):
                return True
        
        return False

# ============= MAIN =============
def main():
    Logger.info(f"Iniciando v2.3 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    bot = BotTelegram()
    historial = GestorHistorial()
    extractor = ExtractorTelegramWeb()
    
    total_enviados = 0
    canales_con_mensajes = 0
    
    for username, config in CONFIG_CANALES.items():
        Logger.info(f"── Procesando {config['nombre']} ({username})...")
        
        mensajes = extractor.obtener_mensajes(username)
        
        if not mensajes:
            # ← MEJORA: distingue entre "no hay mensajes nuevos" y "falló el scraping"
            Logger.warn(f"Sin mensajes extraídos de {username} — saltando canal")
            continue
        
        enviados_canal = 0
        
        for msg in mensajes:
            if not FiltroContenido.pasa_blacklist(msg['texto']):
                Logger.info(f"⏭️ Blacklist: {msg['texto'][:40]}...")
                continue
            
            if not FiltroContenido.tiene_keywords(msg['texto'], config.get('keywords', [])):
                Logger.info(f"⏭️ Sin keyword: {msg['texto'][:40]}...")
                continue
            
            msg_id = msg['id_hash']
            
            if not historial.es_nuevo(username, msg_id):
                Logger.info(f"⏭️ Ya enviado (hash {msg_id}): {msg['texto'][:40]}...")
                continue
            
            texto_formateado = formatear_texto(msg['texto'], config)
            header = crear_header(config)
            
            canal_en_lista = username in CANALES_IMAGENES_PUBLICAS
            canal_con_flag = config.get('imagenes_publicas', False)
            
            incluir_img = False
            url_img = None
            
            if msg['imagen'] and (canal_en_lista or canal_con_flag):
                url_img = msg['imagen']
                incluir_img = True
                Logger.info(f"🖼️  Imagen detectada")
            
            footer = crear_footer(incluir_img, url_img)
            mensaje_final = f"{header}{texto_formateado}{footer}"
            
            if bot.enviar_mensaje(mensaje_final):
                enviados_canal += 1
                total_enviados += 1
                Logger.ok(f"Enviado: {msg['texto'][:60]}...")
                time.sleep(DELAY_ENTRE_MENSAJES)
            else:
                Logger.error(f"Fallo al enviar: {msg['texto'][:40]}...")
        
        if enviados_canal > 0:
            canales_con_mensajes += 1
        
        Logger.ok(f"Canal {config['nombre']}: {enviados_canal} enviados de {len(mensajes)} extraídos")
    
    historial.guardar()
    Logger.info(f"🏁 FIN — {total_enviados} mensajes en {canales_con_mensajes} canales")
    return total_enviados

if __name__ == "__main__":
    import requests
    main()

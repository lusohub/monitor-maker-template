import os
import json
import requests
import redis
from google.cloud import pubsub_v1
import time

class MonitorService:
    def get_crypto_price(self, symbol):
        # Usa a API pública da Binance para obter preços
        try:
            # Normaliza o símbolo (ex: BTC -> BTCUSDT)
            symbol_pair = f"{symbol.upper()}USDT"
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol_pair}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                return float(data['price'])
            else:
                print(f"Erro na API Binance: {response.text}")
                return None
        except Exception as e:
            print(f"Erro ao consultar crypto: {e}")
            return None

    def get_weather(self, location):
        # Usa wttr.in para obter tempo em JSON (não requer chave de API)
        try:
            url = f"https://wttr.in/{location}?format=j1"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                current = data['current_condition'][0]
                return {
                    "temp_C": current['temp_C'],
                    "desc": current['weatherDesc'][0]['value'],
                    "humidity": current['humidity']
                }
            return None
        except Exception as e:
            print(f"Erro ao consultar tempo: {e}")
            return None

def send_to_discord(webhook_url, content):
    try:
        data = {"content": content}
        response = requests.post(webhook_url, json=data)
        response.raise_for_status()
        print(f"Enviado para Discord: {response.status_code}")
    except Exception as e:
        print(f"Erro ao enviar para Discord: {e}")

def callback(message):
    print(f"Mensagem recebida: {message.data}")
    
    try:
        # Decodifica a mensagem que vem do app.py
        data = json.loads(message.data.decode('utf-8'))
        msg_type = data.get('type')
        webhook_url = os.environ.get('DISCORD_URL')
        
        response_text = ""
        should_alert = False

        if msg_type == 'crypto':
            symbol = data.get('symbol')
            threshold = data.get('threshold')
            alert_enabled = data.get('alert_enabled')
            
            price = monitor.get_crypto_price(symbol)
            
            if price:
                response_text = f"💰 **Monitor Crypto**: O preço de **{symbol}** é de **${price:,.2f}**."
                
                # Lógica de Alerta (se o utilizador definiu threshold)
                if alert_enabled and threshold:
                    if price > threshold:
                        response_text += f"\n📈 ALERTA: O preço está ACIMA de ${threshold}!"
                        should_alert = True
                    elif price < threshold:
                        response_text += f"\n📉 O preço está abaixo do alvo de ${threshold}."
                    else:
                        should_alert = True # Preço exato
                else:
                    should_alert = True # Se não há threshold, mostra sempre o preço atual
            else:
                response_text = f"⚠️ Não foi possível obter o preço para {symbol}."
                should_alert = True

        elif msg_type == 'weather':
            location = data.get('location')
            weather_data = monitor.get_weather(location)
            
            if weather_data:
                response_text = (f"🌤️ **Monitor Tempo**: Em **{location}** estão **{weather_data['temp_C']}°C**.\n"
                               f"Condição: {weather_data['desc']} (Humidade: {weather_data['humidity']}%)")
                should_alert = True
            else:
                response_text = f"⚠️ Não foi possível obter o tempo para {location}."
                should_alert = True
        
        # Envia para o Discord se houver algo para reportar
        if webhook_url and should_alert:
            send_to_discord(webhook_url, response_text)
        elif not webhook_url:
            print("Aviso: DISCORD_URL não configurada.")
        
        # Confirma a mensagem no Pub/Sub para ela não voltar
        message.ack()
        
    except Exception as e:
        print(f"Erro a processar mensagem: {e}")
        # message.nack() # Descomentar se quiseres que o PubSub tente reenviar em caso de erro

def main():
    global redis_client
    global monitor
    
    # Inicializa o serviço de monitorização
    monitor = MonitorService()
    
    # --- Configuração Redis (Mantida do original para compatibilidade) ---
    redis_host = os.environ.get('REDIS_HOST')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_auth_string = os.environ.get('REDIS_AUTH_STRING')

    if redis_host:
        print(f"A conectar ao Redis em {redis_host}:{redis_port}...")
        try:
            redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                password=redis_auth_string, 
                decode_responses=True,
                socket_connect_timeout=5
            )
            redis_client.ping()
            print("Conectado ao Redis com sucesso")
        except Exception as e:
            print(f"Aviso: Falha ao conectar ao Redis ({e}). O Worker vai continuar sem cache.")
    
    # --- Configuração Pub/Sub ---
    project_id = os.environ.get('GCP_PROJECT_ID')
    subscription_id = os.environ.get('PUBSUB_SUBSCRIPTION_ID')
    
    if not project_id or not subscription_id:
        print("Erro: GCP_PROJECT_ID e PUBSUB_SUBSCRIPTION_ID têm de estar definidos.")
        return
    
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    
    print(f"A escutar na subscrição: {subscription_path}")
    
    # Limita o fluxo para não sobrecarregar
    flow_control = pubsub_v1.types.FlowControl(max_messages=1)
    
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=callback,
        flow_control=flow_control
    )
    
    print("Monitor ativo e à espera de pedidos...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("A parar serviço...")

if __name__ == "__main__":
    main()
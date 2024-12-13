import boto3
import json
import logging

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ecs_client = boto3.client('ecs')

def lambda_handler(event, context):
    try:
        logger.info(f"Evento recebido: {json.dumps(event)}")
        
        # Extrair informações do evento
        detail = event.get("detail", {})
        cluster = detail.get("clusterArn", "").split("/")[-1]
        service = detail.get("group", "").replace("service:", "")
        
        if not cluster or not service:
            logger.error("Informações de cluster ou serviço ausentes no evento.")
            return {"status": "Erro", "message": "Evento inválido"}
        
        # Obter a configuração atual do serviço
        response = ecs_client.describe_services(
            cluster=cluster,
            services=[service]
        )
        services = response.get("services", [])
        if not services:
            logger.error(f"Serviço {service} não encontrado no cluster {cluster}.")
            return {"status": "Erro", "message": "Serviço não encontrado"}
        
        service_config = services[0]
        current_capacity_providers = service_config.get("capacityProviderStrategy", [])
        
        # Verificar se já está usando FARGATE_SPOT
        is_fargate_spot = any(cp.get("capacityProvider") == "FARGATE_SPOT" for cp in current_capacity_providers)
        
        if is_fargate_spot:
            logger.info(f"Serviço {service} já está usando FARGATE_SPOT.")
            return {"status": "OK", "message": "Sem alterações necessárias"}
        
        # Atualizar o serviço para usar FARGATE_SPOT
        logger.info(f"Atualizando o serviço {service} para usar FARGATE_SPOT.")
        ecs_client.update_service(
            cluster=cluster,
            service=service,
            capacityProviderStrategy=[
                {"capacityProvider": "FARGATE_SPOT", "weight": 1}
            ]
        )
        
        logger.info(f"Serviço {service} atualizado com sucesso.")
        return {"status": "OK", "message": f"Serviço {service} atualizado para FARGATE_SPOT"}
    
    except Exception as e:
        logger.error(f"Erro ao processar o evento: {str(e)}")
        return {"status": "Erro", "message": str(e)}
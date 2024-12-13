import boto3
import json
import logging

# Configuração do logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ecs_client = boto3.client('ecs')
codedeploy_client = boto3.client('codedeploy')

def lambda_handler(event, context):
    """
    Manipula eventos em lote enviados pelo SQS.
    """
    try:
        records = event.get("Records", [])
        if not records:
            logger.warning("Nenhuma mensagem encontrada no evento.")
            return {"status": "OK", "message": "Nenhum evento processado"}

        # Processar cada mensagem
        results = []
        for record in records:
            message = json.loads(record["body"])
            result = process_message(message)
            results.append(result)
        
        return {"status": "OK", "results": results}

    except Exception as e:
        logger.error(f"Erro ao processar mensagens: {str(e)}")
        return {"status": "Erro", "message": str(e)}


def process_message(message):
    """
    Processa uma única mensagem do SQS.
    """
    try:
        logger.info(f"Processando mensagem: {json.dumps(message)}")
        
        # Extrair informações do evento
        detail = message.get("detail", {})
        cluster = detail.get("clusterArn", "").split("/")[-1]
        service = detail.get("group", "").replace("service:", "")
        
        if not cluster or not service:
            logger.error("Informações de cluster ou serviço ausentes no evento.")
            return {"status": "Erro", "message": "Evento inválido"}
        
        # Verificar se há deployments ativos
        if has_active_deployment(cluster, service):
            logger.warning(f"Serviço {service} tem um deployment ativo. Aguardando conclusão.")
            return {"status": "Aguardando", "message": "Deployment ativo detectado"}
        
        # Obter configuração do serviço
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
        logger.error(f"Erro ao processar mensagem: {str(e)}")
        return {"status": "Erro", "message": str(e)}


def has_active_deployment(cluster, service):
    """
    Verifica se há um deployment ativo no serviço ECS associado ao CodeDeploy.
    """
    try:
        response = codedeploy_client.list_deployments(
            applicationName=f"{cluster}-{service}",
            deploymentGroupName=f"{service}-deployment-group",
            includeOnlyStatuses=["Created", "Queued", "InProgress"]
        )
        active_deployments = response.get("deployments", [])
        if active_deployments:
            logger.info(f"Deployments ativos encontrados para {service}: {active_deployments}")
            return True
        return False
    except Exception as e:
        logger.error(f"Erro ao verificar deployments ativos: {str(e)}")
        return False

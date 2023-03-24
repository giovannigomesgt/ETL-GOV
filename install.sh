#!/bin/bash
# Cria o cluster do Kubernetes com Kind
# Verifica se o cluster kind-cluster existe
if kind get clusters | grep -q kind-cluster; then
echo "----------------------------------------------------------------------------------------------------"
echo "O cluster kind-cluster já existe"
echo "----------------------------------------------------------------------------------------------------"
else
# O cluster kind-cluster não existe, exiba uma mensagem de aviso
echo "----------------------------------------------------------------------------------------------------"
echo "Criando um cluster com o nome kind-cluster"
echo "----------------------------------------------------------------------------------------------------"
kind create cluster --config infra/cluster.yaml
fi
# Cria o namespace airflow
# Verifica se o namespace airflow existe
if kubectl get namespace airflow &> /dev/null; then
echo "----------------------------------------------------------------------------------------------------"
echo "O namespace airflow Já existe"
echo "----------------------------------------------------------------------------------------------------"
else
# O namespace airflow não existe, exiba uma mensagem de aviso
echo "----------------------------------------------------------------------------------------------------"
echo "Criando um namespace airflow"
echo "----------------------------------------------------------------------------------------------------"
kubectl create ns airflow
fi
# Adiciona o repositório do Apache Airflow ao Helm
echo "----------------------------------------------------------------------------------------------------"
echo "Adicionando o repositório do Airflow"
echo "----------------------------------------------------------------------------------------------------"
helm repo add apache-airflow https://airflow.apache.org
# Atualiza a lista de repositórios do Helm
echo "----------------------------------------------------------------------------------------------------"
echo "Atualizando todos os repositórios"
echo "----------------------------------------------------------------------------------------------------"
helm repo update
# Instala ou atualiza a instância do Apache Airflow com o Helm
if helm ls -n airflow | grep -q airflow; then
# Exclui a release do Airflow
echo "----------------------------------------------------------------------------------------------------"
echo "A release do Airflow já existe"
echo "----------------------------------------------------------------------------------------------------"
else
echo "----------------------------------------------------------------------------------------------------"
echo "A release do Airflow não existe"
echo "----------------------------------------------------------------------------------------------------"
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f airflow/override-values.yaml \
  --debug \
  --timeout 20m0s
fi
echo "----------------------------------------------------------------------------------------------------"
echo "Iniciando o serviço WebServer Airflow"
echo "----------------------------------------------------------------------------------------------------"
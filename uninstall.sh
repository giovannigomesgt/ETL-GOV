#!/bin/bash

# Verifica se o cluster do Kubernetes está acessível
if kubectl cluster-info &> /dev/null; then
  
  # O cluster está acessível, prossiga com as outras etapas
  echo "----------------------------------------------------------------------------------------------------"
  echo "O cluster do Kubernetes está acessível"
  echo "----------------------------------------------------------------------------------------------------"
  
  # Verifica se a release do Airflow existe
    if helm ls -n airflow | grep -q airflow; then
    # Exclui a release do Airflow
    helm delete airflow -n airflow
    else
    echo "----------------------------------------------------------------------------------------------------"
    echo "A release do Airflow não existe"
    echo "----------------------------------------------------------------------------------------------------"
    fi
    
    # Verifica se o namespace airflow existe
    if kubectl get namespace airflow &> /dev/null; then
    # O namespace airflow existe, exclua-o
    kubectl delete namespace airflow
    echo "----------------------------------------------------------------------------------------------------"
    echo "O namespace airflow foi excluído com sucesso"
    echo "----------------------------------------------------------------------------------------------------"
    else
    # O namespace airflow não existe, exiba uma mensagem de aviso
    echo "----------------------------------------------------------------------------------------------------"
    echo "O namespace airflow não existe"
    echo "----------------------------------------------------------------------------------------------------"
    fi
  
    # Verifica se o cluster kind-cluster existe
    if kind get clusters | grep -q kind-cluster; then
    # O cluster kind-cluster existe, exclua-o
    kind delete cluster --name kind-cluster
    echo "----------------------------------------------------------------------------------------------------"
    echo "O cluster kind-cluster foi excluído com sucesso"
    echo "----------------------------------------------------------------------------------------------------"
    else
    # O cluster kind-cluster não existe, exiba uma mensagem de aviso
    echo "----------------------------------------------------------------------------------------------------"
    echo "O cluster kind-cluster não existe"
    echo "----------------------------------------------------------------------------------------------------"
    fi
    # Removendo imagens do Docker
    echo "----------------------------------------------------------------------------------------------------"
    echo "Removendo imagens"
    echo "----------------------------------------------------------------------------------------------------"
    docker rmi $(docker images -a -q)

else
  # O cluster não está acessível, exiba uma mensagem de erro
  echo "----------------------------------------------------------------------------------------------------"
  echo "O cluster do Kubernetes não está acessível"
  echo "----------------------------------------------------------------------------------------------------"
fi
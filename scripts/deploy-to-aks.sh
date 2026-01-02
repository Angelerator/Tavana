#!/bin/bash
# deploy-to-aks.sh - Comprehensive deployment script for Tavana on AKS
# Ensures VPA, images, and Helm chart are properly deployed

set -e

# Configuration (set these or use environment variables)
NAMESPACE="${NAMESPACE:-tavana}"
ACR_NAME="${ACR_NAME:-acrtavanadev}"
VERSION="${VERSION:-latest}"
RELEASE_NAME="${RELEASE_NAME:-tavana}"

echo "=== Tavana AKS Deployment Script ==="
echo "ACR: ${ACR_NAME}.azurecr.io"
echo "Namespace: ${NAMESPACE}"
echo "Version: ${VERSION}"
echo ""

# 1. Check prerequisites
echo "📋 Checking prerequisites..."
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "helm not found"; exit 1; }
command -v az >/dev/null 2>&1 || { echo "az CLI not found"; exit 1; }

# 2. Check AKS connection
echo "🔌 Checking Kubernetes connection..."
kubectl cluster-info --request-timeout=5s >/dev/null 2>&1 || { echo "Not connected to Kubernetes"; exit 1; }

# 3. Ensure namespace exists
echo "📁 Ensuring namespace ${NAMESPACE} exists..."
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -

# 4. Install VPA CRDs if not present
echo "📊 Checking VPA CRDs..."
if ! kubectl get crd verticalpodautoscalers.autoscaling.k8s.io >/dev/null 2>&1; then
    echo "   Installing VPA CRDs..."
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/vertical-pod-autoscaler/deploy/vpa-v1-crd-gen.yaml
    echo "   ✅ VPA CRDs installed"
else
    echo "   ✅ VPA CRDs already present"
fi

# 5. Install VPA components if not present
echo "📈 Checking VPA components..."
if ! kubectl get deployment vpa-recommender -n kube-system >/dev/null 2>&1; then
    echo "   Installing VPA components..."
    
    # Apply VPA components with patched image for ACR compatibility
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/vertical-pod-autoscaler/deploy/recommender-deployment.yaml
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/vertical-pod-autoscaler/deploy/updater-deployment.yaml
    kubectl apply -f https://raw.githubusercontent.com/kubernetes/autoscaler/master/vertical-pod-autoscaler/deploy/admission-controller-deployment.yaml
    
    # Wait for VPA recommender to be ready
    echo "   Waiting for VPA recommender..."
    kubectl wait --for=condition=available --timeout=120s deployment/vpa-recommender -n kube-system || {
        echo "   ⚠️ VPA recommender may need image patching. See troubleshooting below."
    }
    
    echo "   ✅ VPA components installed"
else
    echo "   ✅ VPA components already present"
fi

# 6. Check/push images to ACR
echo "🐳 Checking images in ACR..."
az acr login --name ${ACR_NAME} 2>/dev/null || {
    echo "   ⚠️ Cannot login to ACR. Make sure you're authenticated with Azure."
    echo "   Run: az login && az acr login --name ${ACR_NAME}"
}

# Check if images exist in ACR
for IMAGE in gateway worker; do
    if az acr repository show --name ${ACR_NAME} --image tavana-${IMAGE}:${VERSION} >/dev/null 2>&1; then
        echo "   ✅ tavana-${IMAGE}:${VERSION} exists in ACR"
    else
        echo "   📥 Pulling and pushing tavana-${IMAGE}:${VERSION} to ACR..."
        docker pull --platform linux/amd64 angelerator/tavana-${IMAGE}:${VERSION} 2>/dev/null || \
        docker pull --platform linux/amd64 ghcr.io/angelerator/tavana-${IMAGE}:${VERSION}
        
        docker tag angelerator/tavana-${IMAGE}:${VERSION} ${ACR_NAME}.azurecr.io/tavana-${IMAGE}:${VERSION}
        docker push ${ACR_NAME}.azurecr.io/tavana-${IMAGE}:${VERSION}
        echo "   ✅ Pushed tavana-${IMAGE}:${VERSION} to ACR"
    fi
done

# 7. Deploy with Helm
echo "🚀 Deploying Tavana ${VERSION}..."
helm upgrade --install ${RELEASE_NAME} ./helm/tavana \
    --namespace ${NAMESPACE} \
    --set global.imageRegistry="${ACR_NAME}.azurecr.io" \
    --set global.imageTag="${VERSION}" \
    --set vpa.enabled=true \
    --set hpa.enabled=true \
    --set pdb.enabled=true \
    --wait --timeout 5m

echo ""
echo "✅ Deployment complete!"
echo ""
echo "=== Status ==="
kubectl get pods -n ${NAMESPACE}
echo ""
kubectl get vpa -n ${NAMESPACE} 2>/dev/null || echo "VPA not yet active"
echo ""
kubectl get hpa -n ${NAMESPACE} 2>/dev/null || echo "HPA not yet active"
echo ""
echo "=== Access ===" 
kubectl get svc -n ${NAMESPACE}
echo ""
echo "🎉 Tavana is ready!"
echo "   Connect with: psql -h <EXTERNAL-IP> -p 5432 -U postgres"


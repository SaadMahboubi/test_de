#!/bin/bash

# Test script for validating Terraform configurations

# Change to the terraform directory using an absolute path
cd "$(dirname "$0")/../terraform"

# Initialize Terraform
terraform init

# Validate Terraform configuration
terraform validate

# Test HCL syntax for all .tf files
for file in ../terraform/*.tf; do
  terraform fmt -check -diff "$file"
done

# Linting with tflint (assuming tflint is installed)
tflint ../terraform

# Plan Terraform deployment
terraform plan

# Output success message
if [ $? -eq 0 ]; then
  echo "Terraform configurations are valid."
else
  echo "Errors found in Terraform configurations."
fi
#!/bin/bash

# Script de test pour valider les configurations Terraform

# Accéder au répertoire terraform en utilisant un chemin absolu
cd "$(dirname "$0")/../terraform"

# Initialiser Terraform
terraform init

# Valider la configuration Terraform
terraform validate

# Tester la syntaxe HCL pour tous les fichiers .tf
for file in ../terraform/*.tf; do
  terraform fmt -check -diff "$file"
done

# Linting avec tflint (en supposant que tflint est installé)
tflint ../terraform

# Planifier le déploiement Terraform
terraform plan

# Afficher un message de succès
if [ $? -eq 0 ]; then
  echo "Les configurations Terraform sont valides."
else
  echo "Des erreurs ont été trouvées dans les configurations Terraform."
fi
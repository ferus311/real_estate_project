#!/bin/bash

# Django Backend Setup Script
# Creates a Django project for Real Estate Backend

set -e

echo "🚀 Setting up Django Backend for Real Estate Project..."

# Configuration
PROJECT_NAME="realestate_backend"
APP_NAME="properties"
BACKEND_DIR="/home/fer/data/real_estate_project/backend"

# 1. Create backend directory
echo "📁 Creating backend directory..."
mkdir -p $BACKEND_DIR
cd $BACKEND_DIR

# 2. Create virtual environment
echo "🐍 Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# 3. Install Django and dependencies
echo "📦 Installing Django and dependencies..."
pip install --upgrade pip
pip install django
pip install djangorestframework
pip install psycopg2-binary  # PostgreSQL adapter
pip install django-cors-headers  # CORS support
pip install django-filter  # Advanced filtering
pip install python-decouple  # Environment variables
pip install dj-database-url  # Database URL parsing

# 4. Create requirements.txt
echo "📝 Creating requirements.txt..."
pip freeze > requirements.txt

# 5. Create Django project
echo "🏗️ Creating Django project..."
django-admin startproject $PROJECT_NAME .

# 6. Create properties app
echo "📱 Creating properties app..."
cd $PROJECT_NAME
python manage.py startapp $APP_NAME

echo "✅ Django project created successfully!"
echo ""
echo "📋 Next steps:"
echo "1. cd $BACKEND_DIR"
echo "2. source venv/bin/activate"
echo "3. Configure settings.py"
echo "4. Create models.py"
echo "5. Run migrations"
echo "6. Start development server"
echo ""
echo "🔧 Configuration files to update:"
echo "- $BACKEND_DIR/$PROJECT_NAME/settings.py"
echo "- $BACKEND_DIR/$PROJECT_NAME/$APP_NAME/models.py"
echo "- $BACKEND_DIR/$PROJECT_NAME/$APP_NAME/views.py"
echo "- $BACKEND_DIR/$PROJECT_NAME/$APP_NAME/urls.py"

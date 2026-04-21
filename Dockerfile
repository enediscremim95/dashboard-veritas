FROM python:3.11-slim

# Instalar Node.js (necessário para npx wrangler)
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar wrangler globalmente
RUN npm install -g wrangler

# Instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar scripts e assets
COPY generate_tratorval.py .
COPY generate_qualy_usa.py .
COPY generate_all.py .
COPY assets/ ./assets/
COPY index.html .

CMD ["python", "generate_all.py"]

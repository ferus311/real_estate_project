# ⚙️ Stage 1: Build
FROM node:20 AS builder

# Làm việc trong thư mục app
WORKDIR /app

# Copy toàn bộ vào container
COPY . .

# Cài dependencies
RUN npm install

# Build production
RUN npm run build

# ⚙️ Stage 2: Serve bằng Nginx
FROM nginx:alpine

# Copy file build vào thư mục public của Nginx
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy file cấu hình nginx (nếu có)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Mở port 80
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]

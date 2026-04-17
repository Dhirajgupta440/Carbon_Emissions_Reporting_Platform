FROM nginx:alpine

# Copy frontend files to nginx html directory
COPY . /usr/share/nginx/html/

# Expose nginx default port
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
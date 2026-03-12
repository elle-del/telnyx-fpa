# FP&A Dashboard - Static site served by nginx
FROM nginx:alpine

# Update packages to get latest security patches
RUN apk update && apk upgrade --no-cache

# Copy static files
COPY *.html /usr/share/nginx/html/
COPY data/ /usr/share/nginx/html/data/

# Custom nginx config for SPA routing
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Health check endpoint
RUN echo "OK" > /usr/share/nginx/html/health

EXPOSE 8080

# Run as non-root
RUN chown -R nginx:nginx /usr/share/nginx/html && \
    chown -R nginx:nginx /var/cache/nginx && \
    touch /var/run/nginx.pid && \
    chown nginx:nginx /var/run/nginx.pid

USER nginx

CMD ["nginx", "-g", "daemon off;"]

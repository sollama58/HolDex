# Use a lightweight Node.js version
FROM node:18-alpine

# Create app directory
WORKDIR /usr/src/app

# Create data directory for persistence
# This is crucial for SQLite to survive restarts
RUN mkdir -p ./data

# Copy package files first to leverage Docker layer caching
COPY package*.json ./

# Install production dependencies only to keep image small
RUN npm install --only=production

# Copy the rest of the application source code
COPY . .

# Expose the API port
EXPOSE 3000

# Define a volume for the data directory
# This ensures data persists if the container is stopped/removed
VOLUME ["/usr/src/app/data"]

# Define command to run the app
CMD [ "npm", "start" ]

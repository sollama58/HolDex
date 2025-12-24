# Use a lightweight Node.js version
FROM node:18-alpine

# Create app directory inside the container
WORKDIR /usr/src/app

# Copy package files first to leverage Docker layer caching
# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json ./

# Install production dependencies only to keep the image small
RUN npm install --only=production

# Copy the rest of the application source code
COPY . .

# Expose the API port
EXPOSE 3000

# Define the command to run the app
CMD [ "npm", "start" ]

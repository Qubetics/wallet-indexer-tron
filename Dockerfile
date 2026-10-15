FROM node:24.21.0

# Create app directory
WORKDIR /usr/src/app
# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
RUN npm install -g ts-node
COPY package*.json ./
RUN npm ci
# RUN mkdir -p /usr/src/app/s3
# If you are building your code for production

# Bundle app source
COPY . .
CMD [ "npm", "run", "start" ]
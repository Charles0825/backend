FROM node:20.16-alpine3.19

# Create app directory
WORKDIR /usr/src/app

# A wildcard is used to ensure both package.json AND package-lock.json are copied
COPY package*.json .

# Install app dependencies
RUN npm ci --omit=dev && npm cache clean --force

# Bundle app source
COPY . .

EXPOSE 5555

CMD [ "node", "server.js" ]

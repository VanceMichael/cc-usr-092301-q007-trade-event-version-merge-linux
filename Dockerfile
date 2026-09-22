FROM node:22-bookworm
WORKDIR /app
COPY package.json ./
RUN npm install
COPY src ./src
COPY test ./test
EXPOSE 3000
CMD ["npm", "start"]

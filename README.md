# README

This project is a small Node.js / Express.js based REST API example designed to serve up Speed Management Network data once it has passed through the Data Broker (<http://github.com/RISBIC>) project into a PostgreSQL database.

`app.js` bootstraps the app and then loads all `js` files in the `modules/` and passes them an instance of the Express.js `app` object and the any-db `pool` object.
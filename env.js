import dotenv from "dotenv";
dotenv.config();

export const HOST = process.env.HOST;
export const PORT = process.env.PORT;
export const DATABASE = process.env.DATABASE;
export const USER = process.env.USER;
export const PASSWORD = process.env.PASSWORD;
export const DATABASE_URL = process.env.DATABASE_URL;

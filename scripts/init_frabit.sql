-- create a user to connected DB server
CREATE USER 'frabit'@'localhost' IDENTIFIED BY 'Frabit@123';
GRANT INSERT, UPDATE, DELETE, SELECT ON frabit.* TO 'frabit'@'localhost';

-- create frabit database used store backup policy and relvent info
CREATE DATABASE frabit /*!40100 DEFAULT CHARACTER SET utf8mb4 */;

-- create tables within frabit

CREATE TABLE public.tickets(
id int,
first_name varchar(20),
last_name varchar(20),
email varchar(50),
gender varchar(20),
ip_address varchar(20),
date_load timestamp default current_timestamp
)
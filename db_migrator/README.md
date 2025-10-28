# pythonDataMigrator

This is light python tool to Insert, Update and Validate data in PostgreSql databases in reliable way.
The application allows you configure backup table, update/insert query, query params and pre/post validations.
There are two config files :
   1) db_config.ini : Add database details here.
   2) job_config.json : Add Backup details, draft_query, Params and Validation query here.
   3) Pass db_config.ini & job_config.json as arguments to the python application.
   4) It creates version 1 backup table, updates given table as version 2 and runs pre as well as post validations.
validation config where expection values after insert is 1
<img width="901" height="271" alt="image" src="https://github.com/user-attachments/assets/371e29d7-08eb-4d77-a4de-ff2489b916cb" />

as results matched : 
<img width="416" height="157" alt="image" src="https://github.com/user-attachments/assets/a3943902-ee8a-4239-aea5-1a0d9cbe4e98" />

Data table inserted as expected - 
<img width="933" height="90" alt="image" src="https://github.com/user-attachments/assets/ceaf804d-b8ef-4ee2-8c6b-5980256c150e" />

**Docker Run Commands - **
1) Open cmd, goto project location
2) Run to build docker image - /> docker build -t db_mgtor_image .
3) Run docker container with built image - />docker run --network=host -it db_mgtor_image bash -c "bash"
4) Run app command in docker prompt - root@docker-desktop:/usr/app/src# python db_mgtor.py db_config.ini jobConfig.json

      
      

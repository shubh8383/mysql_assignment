
import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Shubh@9765",
    database="my_database")

curr=conn.cursor()

#curr.execute("""""")


conn.commit()

curr.execute("""select * from linkedin_users """)
em=curr.fetchall()
print(em)

#linkedin_data table task no-1
'''
#to create table
curr.execute("""create table linkedin_data(lid int, full_name varchar(30), 
        contact_no int(10), education varchar(20), exp float,
        designation varchar(30), job_st_dt date)""")

#to find id of max(exp)
curr.execute("""select lid from linkedin_data where exp=(select max(exp) from linkedin_data) """)

#Find lid, fullname,  whose name ends and  starts with a
curr.execute("""select lid, fullname from linkedin_data where full_name like 'a%a' """)

#to get data job_st_dt between 2020 to 2021
curr.execute("""select * from linkedin_data where job_st_dt between '2020-01-01' and '2021-12-31' """)

#to find id of min(date)
curr.execute("""select * from linkedin_data where exp=(select min(job_st_dt) from linkedin_data) """)

'''

#linkedin_member_details task no-2
'''
curr.execute("""create table linkedin_data(lid int, first_name varchar(30),
        last_name varchar(30),
        DOB date, email varchar(30), bio text,
        website_url varchar(30), company_name varchar(30),
        sector varchar(20), current_status boolean, last_organization varchar(30))""")

#to fetch data from both tables
curr.execute("""select * from linkedin_data cross join linkedin_memember_details""")

'''


#unique_emails task no-3

'''
curr.execute("""create table unique_emails(lid int, email varchar(30), email_right varchar(20), 
            email_left varchar(20), email_domain varchar(20), 
            fullname varchar(30), first_name varchar(20), last_name varchar(20), 
            firstname_lc int, lastname_lc int, fullname_length int)""")

#creating first name and last name using fullname column only
curr.execute("""update unique_emails set first_name=SUBSTRING_INDEX(fullname,' ', 1), 
                last_name=SUBSTRING_INDEX(fullnamem,' ',-1)""")

#updating email_rigth, email_left and email_domain with the help of email column
curr.execute("""update unique_emails set email_right=SUBSTRING_INDEX(email, ".", -1) 
                email_left=SUBSTRING_INDEX(email, "@", 1),
                email_domain=SUBSTRING_INDEX(SUBSTRING_INDEX("email", "@", -1), ".",1 ) where lid=lid""")



curr.execute("""alter table unique_emails add name_to_long boolean""")

curr.execute("""update unique_emails set name_to_long= CASE
                WHEN length(fullname)>=10 THEN True
                ELSE False
                END """)
'''


#unique_emails Task no-4-

'''
#add new column created_at in unique_emails
curr.execute(""" alter table unique_emails add created_at date""")

#add new column updated_at with current timestamp in unique_emails
curr.execute("""alter table unique_emails add updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;""")

#create new column fname_start_with_a in unique_emails
curr.execute("""alter table unique_emails add fname_start_with_a varchar(20)""")

#update fname_start_with_a whose fname start with a
curr.execute("""update unique_emails set fname_start_with_a= fullname where fname like 'a%' """)

#fetch all data whose updated_at between 24 hrs
curr.execute("""select * from unique_emails where created_at > now() - interval 1 day  """)

#get count of all records group_by created_at
curr.execute(""" select count(*) from unique_emails group by created_at  """)

#change email domain to co.in whose email start with 'a', 'v', 's' and 'm'
curr.execute("""update unique_emails set email_domain='co.in' where email like 'a%', email like 's%', email like 'v%', email like 'm%' """)

#to check min dob and get all record of that person
curr.execute(""" select * from unique_emails where bob=(select min(dob) from unique_emails) """)

#to check max email count
curr.execute("""select max(length(email)) from unique_emails""")


'''








#UNIQUE_EMAILS task no- 5

'''
curr.execute("""update unique_emails set unique_emails_staging= CASE
                WHEN length(email_left)>3 and length(email_left)<=5 THEN 'small',
                WHEN length(email_left)>5 and length(email_left)<=8 THEN 'medium',
                WHEN length(email_left)>8 and length(email_left)<=12 THEN 'a too large'
                ELSE 'to big'
                END """)





'''





#student table task no - 6

'''
#list to add data
edu=[{"id":1,'fname':"santosh", 'lname':'bhise',"degree":"12th", "start_date":2012, "end_date":2013},
    {"id":2,'fname':"akash", 'lname':'shitole',"degree":"Bcom", "start_date":2008, "end_date":2011},
    {"id":3,'fname':"laxman", 'lname':'mansure',"degree":"BA", "start_date":2014, "end_date":2016},
    {"id":4,'fname':"vivek", 'lname':'harbade',"degree":"BE", "start_date":2015, "end_date":2019},
    {"id":5,'fname':"mayuri", 'lname':'more',"degree":"BSc", "start_date":2011, "end_date":2014}]

#to convert a list into dataframe
df=pd.DataFrame(edu)

#to create new table
curr.execute("""CREATE TABLE student_table(id int not null,
                    fname varchar(30), lname varchar(30),
                    education varchar(30), education_st_dt year,
                    education_ed_dt year, latest_end_dt year)""")

#to modify column datatype
curr.execute("""alter table employee modify exp_ed_dt year""")

#to add dataframe into table
for row in df.itertuples():
    curr.execute("""insert into student_table(id,fname, lname, education, education_st_dt, education_ed_dt) 
        values(%s, %s, %s, %s, %s, %s)""",(row.id, row.fname, row.lname, row.degree, row.start_date, row.end_date))

#to update latest end date column with the help of education_ed_dt
curr.execute("""update student_table set latest_end_dt=education_ed_dt where id=id""")

conn.commit()

#to fetch all data of student_table
curr.execute("""select * from student_table""")

st=curr.fetchall()

print(st)
'''

#employee table task no-7
'''

insert_into=("""insert into employee(id, emp_position, exp_st_dt, exp_ed_dt) 
        values(%s, %s, %s, %s)""")

emp=[{"id":1,"position":"supervisor", "start_date":2016, "end_date":2019},
        {"id":2,"position":"accountant", "start_date":2013, "end_date":2020},
        {"id":3,"position":"manager", "start_date":2019, "end_date":2022},
        {"id":4,"position":"senior developer", "start_date":2020, "end_date":2022},
        {"id":5,"position":"CEO", "start_date":2014 ,"end_date":2018}]

df=pd.DataFrame(emp)

#curr.execute("""create table employee(id int, emp_fname varchar(20), 
#            emp_lname varchar(20), emp_position varchar(20),
#             emp_exp float, exp_st_dt year, exp_ed_dt float)""")
            
for row in df.itertuples():
    curr.execute(insert_into, (row.id, row.position, row.start_date, row.end_date))


curr.execute("""update employee as e, student_table as s set e.emp_fname=s.fname, e.emp_lname=s.lname where e.id=s.id""")

#emp_exp column update it with difference of experience end- start
curr.execute("""update employee set emp_exp =exp_ed_dt-exp_st_dt""")

conn.commit()

curr.execute("""select * from employee""")
em=curr.fetchall()
print(em)
'''

#linkedin_users task no-8

'''
#to create linkedin_users table
curr.execute("""create table linkedin_users(id int, fname varchar(20), 
            lname varchar(20), education varchar(20),
            experience float, edu_ed_dt year, exp_st_dt year, 
            exp_ed_dt year, currently_working boolean, all_education varchar(30))""")

#to import fname, lname, etc from student_table
curr.execute("""update linkedin_users as l, student_table as s 
            set l.fname=s.fname,l.lname=s.lname, 
            l.education=s.education, l.edu_ed_dt=s.education_ed_dt where s.id=l.id""")

#to update exp start date from employee table
curr.execute("""update linkedin_users as l, employee as e 
            set l.exp_st_dt=e.exp_st_dt where e.id=l.id""")


#to update boolean values in currently_working column using if conditions
curr.execute("""UPDATE linkedin_users 
      SET currently_working = CASE
         WHEN exp_ed_dt != '' THEN False 
         ELSE True
      END""")

#to create new table exp_years
curr.execute("""alter table linkedin_users add exp_years int""")

#to update exp_years
curr.execute("""UPDATE linkedin_users 
      SET exp_years = CASE
         WHEN exp_ed_dt != '' THEN exp_ed_dt-exp_st_dt 
         ELSE 2022-exp_st_dt
      END""")

conn.commit()

curr.execute("""select * from linkedin_users""")
em=curr.fetchall()
print(em)
'''





ques--(8)Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]?

quary---1st...2011... select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final   where full_time_position ='Y' and year='2011' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc; 

output----FRANCHISE DEVELOPMENT ANALYST	Y	2011	43306.0
MARKET RESEARCH ANALYST (ACCOUNT MANAGER)	Y	2011	43306.0
PRODUCT RESEARCH ANALYST, DAYLIGHTING	Y	2011	43306.0
LOTUS NOTES/DOMINO DEVELOPER	Y	2011	43306.0
TELEVISION/ NEWS VIDEO EDITOR	Y	2011	43306.0
MARKETING/SALES ANALYST	Y	2011	43306.0

2nd...2011...select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N'and year='2011' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

ARM MANAGER	N	2011	42931.0
ESTIMATOR	N	2011	42924.0
ASSISTANT RESEARCH SCIENTIST	N	2011	42916.90476190476
QUALITY CONTROL CHEMIST	N	2011	42910.0
IMPORT & CONTRACT SPECIALIST	N	2011	42889.0

2012-----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='Y' and year='2012' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2012---select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N' and year='2012' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2013----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='Y' and year='2013' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2013----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N' and year='2013' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2014-----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='Y' and year='2014' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2014----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N' and year='2014' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2015----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='Y' and year='2015' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2015-----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N' and year='2015' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2016----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='Y' and year='2016' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;

2016----select job_title,full_time_position,year,avg(prevailing_wage) as average from h1b_final where full_time_position ='N' and year='2016' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc;





ques--2(b)find top 5 locations in the US who have got certified visa for each year.[certified]?

quary----2011--select worksite,count(case_status)as temp,year from h1b_final where year ='2011' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;

output---NEW YORK, NEW YORK	23172	2011
HOUSTON, TEXAS	8184	2011
CHICAGO, ILLINOIS	5188	2011
SAN JOSE, CALIFORNIA	4713	2011
SAN FRANCISCO, CALIFORNIA	4711	2011
Time taken: 385.259 seconds, Fetched: 5 row(s)

2012----select worksite,count(case_status)as temp,year from h1b_final where year ='2012' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;

2013----select worksite,count(case_status)as temp,year from h1b_final where year ='2013' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;

2014----select worksite,count(case_status)as temp,year from h1b_final where year ='2014' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;

2015----select worksite,count(case_status)as temp,year from h1b_final where year ='2015' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;

2016----select worksite,count(case_status)as temp,year from h1b_final where year ='2016' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5;



ques----5(a) Find the most popular top 10 job positions for H1B visa applications for each year?
a) for all the applications?
b) for only certified applications?

quary----select job_title,year,count(job_title)as temp from h1b_final where year='2011' group by job_title,year order by temp desc limit 10; 

output------PROGRAMMER ANALYST	2011	31799
SOFTWARE ENGINEER	2011	12763
COMPUTER PROGRAMMER	2011	8998
SYSTEMS ANALYST	2011	8644
BUSINESS ANALYST	2011	3891

2012-----select job_title,year,count(job_title)as temp from h1b_final where year='2012' group by job_title,year order by temp desc limit 10; 

2013-----select job_title,year,count(job_title)as temp from h1b_final where year='2013' group by job_title,year order by temp desc limit 10; 

2014----select job_title,year,count(job_title)as temp from h1b_final where year='2014' group by job_title,year order by temp desc limit 10; 

2015----select job_title,year,count(job_title)as temp from h1b_final where year='2015' group by job_title,year order by temp desc limit 10;

2016----select job_title,year,count(job_title)as temp from h1b_final where year='2016' group by job_title,year order by temp desc limit 10;


5(b)---for only certified applications?

quary----5(b)-----select job_title,year,count(job_title)as temp from h1b_final where case_status='CERTIFIED' group by job_title,year order by temp desc limit 10;

output----
PROGRAMMER ANALYST	2015	48203
PROGRAMMER ANALYST	2016	47964
PROGRAMMER ANALYST	2014	38625
PROGRAMMER ANALYST	2013	29906
PROGRAMMER ANALYST	2012	29226
PROGRAMMER ANALYST	2011	28806
SOFTWARE ENGINEER	2016	25890
SOFTWARE ENGINEER	2015	23352
SOFTWARE ENGINEER	2014	17278
COMPUTER PROGRAMMER	2014	13796

question----(7) Create a bar graph to depict the number of applications for each year [All]?

quary-----select year,count(*) from h1b_final group by year order by year;

output-----
2011	358767
2012	415607
2013	442114
2014	519427
2015	618727
2016	647803




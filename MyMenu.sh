#!/bin/bash 

show_menu()

{

     NORMAL=`echo "\033[m"`

    MENU=`echo "\033[36m"` #Blue

    NUMBER=`echo "\033[33m"` #yellow

    FGRED=`echo "\033[41m"`

    RED_TEXT=`echo "\033[31m"`

    ENTER_LINE=`echo "\033[33m"`

    echo -e "${MENU}**********************H1B_FINAL***********************${NORMAL}"

      echo -e "${MENU}**${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"

    echo -e "${MENU}**${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest growth in applications. ${NORMAL}"

    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"

    echo -e "${MENU}**${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.${NORMAL}"
 
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions?${NORMAL}"

    echo -e "${MENU}**${NUMBER} 6)  ${MENU} Which top 5 employers file the most petitions each year?case status-all ${NORMAL}"

    echo -e "${MENU}**${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for all the applications${NORMAL}"

    echo -e "${MENU}**${NUMBER} 8) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for only certified applications${NORMAL}"

    echo -e "${MENU}**${NUMBER} 9)  ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time.${NORMAL}"
 
    echo -e "${MENU}**${NUMBER} 10)  ${MENU} Create a bar graph to depict the number of applications for each year${NORMAL}"

    echo -e "${MENU}**${NUMBER} 11)   ${MENU}Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order${NORMAL}"

    echo -e "${MENU}**${NUMBER} 12)   ${MENU} Which are employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?${NORMAL}"

    echo -e "${MENU}**${NUMBER} 13)  ${MENU} Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000? ${NORMAL}"

    echo -e "${MENU}**${NUMBER} 14)  ${MENU}Export result for option no 10 to MySQL database.${NORMAL}"

    echo -e "${MENU}*********************************************${NORMAL}"

    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"

    read opt

}

function option_picked() 

{

    COLOR='\033[01;31m' # bold red

    RESET='\033[00;00m' # normal white

    MESSAGE="$1"  #modified to post the correct option selected

    echo -e "${COLOR}${MESSAGE}${RESET}"

}

clear

show_menu

	while [ opt != '' ]

    do

    if [[ $opt = "" ]]; then 

            exit;

    else

        case $opt in

        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
	        	
	        hadoop jar /home/hduser/Documents/DataEngineer.jar DataEngineer /user/hive/warehouse/finalproject/h1b_final /finalproject/outputmapreduce1	
		hadoop fs -cat /finalproject/output4/p*
                show_menu;
        ;;

       2) clear;

        option_picked "1 b) Find top 5 job titles who are having highest growth in applications. ";
                pig /home/hduser/Downloads/question1b.pig
		hadoop fs -cat /pig/question1b/p*
                show_menu;
        ;;  

        3) clear;

        option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
		       
	        hadoop jar /home/hduser/Documents/dataengineer2.jar Dataengineer /user/hive/warehouse/finalproject/h1b_final /finalproject/dataengi2ka4
	        hadoop fs -cat /finalproject/outputmapreduce1/p*	
                show_menu;
        ;;

	    4) clear;

        option_picked "2 b) find top 5 locations in the US who have got certified visa for each year";

hive -f /home/hduser/Downloads/finalproject/table.sql;


	    hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2011' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            
            hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2012' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            
            hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2013' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            
            hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2014' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            
            hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2015' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            
            hive -e "select worksite,count(case_status)as temp,year from finalproject.h1b_final where year ='2016' and case_status ='CERTIFIED' group by worksite,year order by temp desc limit 5";
            show_menu;
        ;;  

	    5) clear;

        option_picked "3) Which industry has the most number of Data Scientist positions";
              
              hadoop jar /home/hduser/Documents/DataScientist.jar DataScientist /user/hive/warehouse/finalproject/h1b_final /finalproject/DScientoutput
               hadoop fs -cat /finalproject/DScientoutput/p*
              show_menu;
        ;;

        6) clear;

        option_picked "4)Which top 5 employers file the most petitions each year?";
	
               hadoop jar /home/hduser/Documents/dataengineer4.jar  Dataengineer /user/hive/warehouse/finalproject/h1b_final /finalproject/output4
        
	        hadoop fs -cat /finalproject/dataengi2ka4/p*	
                show_menu;
        ;;

        7) clear;

        option_picked "5(a) Find the most popular top 10 job positions for H1B visa applications for each year? (for all the applications)";

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2011' group by job_title,year order by temp desc limit 10"; 

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2012' group by job_title,year order by temp desc limit 10";  

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2013' group by job_title,year order by temp desc limit 10";

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2014' group by job_title,year order by temp desc limit 10";  

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2015' group by job_title,year order by temp desc limit 10"; 

                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where year='2016' group by job_title,year order by temp desc limit 10";
 

           option_picked 
        ;;

        8) clear;

	option_picked "5(b) Find the most popular top 10 job positions for H1B visa applications for each year? (for only certified applications)";
                hive -e "select job_title,year,count(job_title)as temp from finalproject.h1b_final where case_status='CERTIFIED' group by job_title,year order by temp desc limit 10";
                show_menu;
        ;;

        9) clear;

       option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time";
                 pig /home/hduser/Downloads/question6.pig
		hadoop fs -cat /pig/question6/p*
                show_menu;
             ;;


      10) clear;

        option_picked "7) Create a bar graph to depict the number of applications for each year";
		hive -e "select year,count(*) as count_star from finalproject.h1b_final group by year order by year;"
                show_menu;
        ;;

        11) clear;

        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate) arrange output in descending order [Certified and Certified Withdrawn]";
	        
                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2011' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc";
                
                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2012' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc"; 
 
                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2013' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc"; 

                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2014' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc"; 

                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2015' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc";

                hive -e "select job_title,full_time_position,year,avg(prevailing_wage) as average from finalproject.h1b_final   where full_time_position ='Y' and year='2016' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year order by average desc"; 
  
  show_menu;
        ;;

	12) clear;

	option_picked "9) Which are   employers who have the highest success rate in petitions more than 70% in petitions and total petions filed more than 1000?";
               
                pig /home/hduser/Downloads/question9.pig
		hadoop fs -cat /pig/question9/p*
                show_menu;
        ;;

	13) clear;
		
        option_picked "10) Which are the top 10 job positions which have the  success rate more than 70% in petitions and total petitions filed more than 1000?";

		pig /home/hduser/Downloads/question10.pig
		hadoop fs -cat /pig/question10/p*
                show_menu;
        ;;

	14) clear;

	option_picked "11) Export result for question no 10 to MySql database";
     
                mysql -u root -p '' -e 'create database h1b_final;use h1b_final;create table question11(job_title varchar(100),success_rate float,petitions int(11);';
                sqoop export --connect jdbc:mysql://localhost/h1b_final --username root --password linux --table question11 --update-mode allowinsert  --export-dir /pig/question10/p*  --input-fields-terminated-by '\t' ;
                echo -e '\n\nDisplay contents from MySQL Database.\n\n'
                echo -e '\n10) Which are the top 10 job positions that have  success rate more than 70% in petitions and total petitions filed more than 1000?\n\n'
                mysql -u root -p '' -e 'select * from h1b_final.question11';
                show_menu;
        ;;

		\n) exit;   

		;;

        *) clear;

        option_picked "Pick an option from the menu";
        show_menu;
        ;;

    esac

fi

done

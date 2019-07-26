# start_time <- Sys.time()

library(sqldf)
library(odbc)
library(sas7bdat)
library(eeptools)
library(dplyr)

setwd("//172.24.38.79/Neha/27 5P/03 BiweeklyCustBureauScrub/PL Leads Check (June & historical)")   #added on 25/04 

source("02 Function to high confidence city.R")


con_sandbox <- dbConnect(odbc(),
                                 Driver = "SQL Server Native Client 11.0",
                                 Server = "10.129.19.4",
                                 Database = "Sandbox",
                                 UID = "5PDataWarehouse",
                                 # PWD = rstudioapi::askForPassword("Database password"),
                                 PWD = "5PDataWarehouse@123",
                                 Port = 8080
)

#############################################################################################################



#################### Check the bureau list and if everything is correct,

##### Enter the current date in the format yyyymmdd ################

create.date  = '20190724'
create.date1 = '2019-07-24'
####################################################################


# query = paste0("select  ClientCode, FirstName, pincode, DateOfBirth, Mobile, PAN,Address,dateofjoining, refresh_flag,New_month_cust,Created_On
#          from 
# 	    
#           (
#       		  select A.ClientCode, 
#       		         A.FirstName, 
#       			       A.pincode, 
#       			       A.DateOfBirth, 
#       				     A.Mobile, 
#       			       A.PAN, 
#                     A.Address1 as Address,
#                   A.dateofjoining
#       		        ,case when B.ClientCode is null then 1 else B.refresh_flag end as refresh_flag
# ,Created_On
# ,case when left(DateOfJoining, len(DateofJoining)-8) between cast('2019-06-01' as Date) and cast('2019-06-30' as Date) then 1 else 0 end as New_month_cust
#       		  from
#       		
#       		 [5PDataWarehouse].[dbo].[Customer] A 
#       		 
#       		 left join
#        			
#       			( select ClientCode,Created_On,
#       						  case when datediff(dd,Created_On,cast('",create.date1,"' as date) ) > 90 then 1 else 0 end as refresh_flag
#       				from Scrub_History_Cust5P ) B
#       		 
#       		 on A.ClientCode = B.ClientCode
#           where A.dateofjoining <= '",create.date1,"'
#       		 
#       		) tbl
# 	where refresh_flag = 1")


query = paste0("Select * from [Sandbox].[dbo].[Customers_to_waterfall]")

all.cust.5p = dbFetch(dbSendQuery(con_sandbox,query))

  # Pincode data 
  pincode.data = read.csv('25.GOI_PINCODE.csv')
  pincode.data = pincode.data[!duplicated(pincode.data$pincode),]
  
  
  # for negative pincodes
  neg.pincode = read.sas7bdat('negative_new.sas7bdat')
 
  # write.csv(neg.pincode, "negative_pincodes.csv")
  
  # FIU pan info
  fiu.pan = read.sas7bdat('fiu_pan.sas7bdat')
  fiu.pan$PAN = as.character(fiu.pan$PAN)
  
  # write.csv(neg.pincode, "negative_pincodes.csv")
  
  # Age Flag 
  
  all.cust.5p$Age[!is.na(all.cust.5p$DateOfBirth)] = age_calc(dob = as.Date(all.cust.5p$DateOfBirth[!is.na(all.cust.5p$DateOfBirth)]), 
                                                              enddate = as.Date(as.Date(create.date1, format = "%Y-%m-%d")),
                                                              units = "years", precise = TRUE)
  
  all.cust.5p$Age = as.integer(all.cust.5p$Age)
  
  all.cust.5p$Age_elg = ifelse(all.cust.5p$Age>=21,ifelse(all.cust.5p$Age<=58,1,0),0)
  sum(all.cust.5p$Age_elg)
  # Negative pincode flag
  all.cust.5p = sqldf("select a.*,
                      case when b.pincode is null then 0 else 1 end as 'negative_pin'
                      from [all.cust.5p] a 
                      left join [neg.pincode] b
                      on a.pincode = b.pincode")
  
  # Update to servicable cities - April 2019
  all.cust.5p = merge(all.cust.5p,pincode.data, by.x = 'Pincode', by.y = 'pincode',  all.x=T)
  
  query2 = paste0("select a.ClientCode, Pincode
                   from [Sandbox].[dbo].[Customers_to_waterfall] a
                   inner join  [Sandbox].[dbo].[PL_offered_leads_till_20190724] b 
                   on a.ClientCode = b.ClientCode
                   group by a.ClientCode, Pincode
                ")
  
  pl_offered_leads = dbFetch(dbSendQuery(con_sandbox,query2))
  
  pl_offered_leads = merge(pl_offered_leads,pincode.data, by.x = 'Pincode', by.y = 'pincode', all.x = TRUE)
  colnames(pl_offered_leads)
  
  new_cities
  
  
  servicable.cities = c( 'AHMEDABAD'
                         ,'DELHI'
                         ,'SOUTH DELHI'
                         ,'EAST DELHI'
                         ,'GHAZIABAD'
                         ,'BANGALORE'
                         ,'BANGALORE RURAL'
                         ,'CHENNAI'
                         ,'COIMBATORE'
                         ,'FARIDABAD'
                         ,'GURGAON'
                         ,'HYDERABAD'
                         ,'CENTRAL DELHI'
                         ,'INDORE'
                         ,'JAIPUR'
                         ,'KOLKATA'
                         ,'LUCKNOW'
                         ,'LUDHIANA'
                         ,'MUMBAI'
                         ,'PUNE'
                         ,'THANE'
                         ,'SOUTH WEST DELHI'
                         ,'VADODARA'
                         ,'VISAKHAPATNAM'
                         ,'RAJKOT'
                         ,'NEW DELHI'
                         ,'NORTH DELHI'
                         ,'NORTH EAST DELHI'
                         ,'NORTH WEST DELHI'
                         ,'WEST DELHI'
                         ,'CENTRAL DELHI'
                         ,'NASHIK'
                         ,'Patna'
                         ,'Kanpur Nagar'
                         ,'North 24 Parganas'
                         ,'Jodhpur'
                         ,'Bhopal'
                         ,'Gautam Buddha Nagar',
                         
                         "ahmedabad",
                         "deesa",
                         "idar",
                         "kalol",
                         "mehsana",
                         "morbi",
                         "nadiad",
                         "palanpur",
                         "radhanpur",
                         "gandhinagar",
                         "baroda",
                         "amreli",
                         "bhavnagar",
                         "bhuj",
                         "gondal",
                         "himmatnagar",
                         "jamnagar",
                         "rajkot",
                         "surendranagar",
                         "silvassa",
                         "bharuch",
                         "gandhidham",
                         "navsari",
                         "surat",
                         "valsad",
                         "vapi",
                         "bhopal",
                         "dewas",
                         "indore",
                         "jabalpur",
                         "raipur",
                         "guwahati",
                         "jhorhat",
                         "bhagalpur",
                         "muzaffarpur",
                         "patna",
                         "bokaro",
                         "dhanbad",
                         "jamsedpur",
                         "ranchi",
                         "angul",
                         "balasore",
                         "bhubaneswar",
                         "cuttack",
                         "puri",
                         "agartala",
                         "asansol",
                         "durgapur",
                         "kolkata",
                         "siliguri",
                         "kalyan",
                         "mumbai",
                         "thane",
                         "pune",
                         "new delhi",
                         "noida",
                         "gurgaon",
                         "chandigarh",
                         "ajmer",
                         "beawar",
                         "kishangarh",
                         "bikaner",
                         "alwar",
                         "baran",
                         "bhilwara",
                         "jaipur",
                         "sikar",
                         "jodhpur",
                         "pali",
                         "bundi",
                         "chittaurgarh",
                         "kota",
                         "udaipur",
                         "lucknow",
                         "bangalore",
                         "chennai",
                         "coimbatore",
                         "erode",
                         "madurai",
                         "salem",
                         "trichy",
                         "hyderabad",
                         "karimnagar",
                         "warangal",
                         "chirala",
                         "bhimavaram",
                         "eluru",
                         "guidvada",
                         "guntur",
                         "kurnool",
                         "nellore",
                         "ongole",
                         "tadepalligudem",
                         "tirupathi",
                         "vijayawada",
                         "kakinada",
                         "rajamhundry",
                         "vizag",
                         "vizianagaram",
                         "gandhi nagar"
                         
                         
                         #Extra cities added on 14-06-2019
                         ,'Davangere'
                         ,'Nanded'
                         ,'Dehradun'
                         ,'Ananthapur'
                         ,'Nagaur'
                         ,'Parbhani'
                         ,'Sabarkantha'
                         ,'Sangli'
                         ,'Jhujhunu'
                         ,'Satara'
                         ,'Belgaum'
                         ,'Raigarh(MH)'
                         ,'Kanchipuram'
                         ,'Jalgaon'
                         ,'Solapur'
                         ,'Chittoor'
                         ,'Ahmed Nagar'
                         ,'Aurangabad'
                         ,'Amravati'
                         ,'Bhiwani'
                         ,'Vellore'
                         ,'Akola'
                         ,'Prakasam'
                         ,'Tumkur'
                         ,'Mahesana'
                         ,'Ganganagar'
                         ,'Jalna'
                         ,'Banaskantha'
                         ,'Varanasi'
                         ,'Kolhapur'
                         ,'Mysore'
                         ,'Gorakhpur'  
                         ,'Davangere'
                         ,'Hisar'
                         ,'Kachchh'
                         ,'Nagpur'
                         
  )
  
  
  # write.csv(servicable.cities, "servicable.cities.csv")
  
  all.cust.5p$servicable_flag = ifelse(tolower(all.cust.5p$City_goi) %in% tolower(servicable.cities),1,0)
  
  
  # FIU pan flag
  all.cust.5p = sqldf("select a.*,
                    case when b.PAN is null then 0 else 1 end as 'fiu_pan'
                    from [all.cust.5p] a
                    left join [fiu.pan] b
                    on a.PAN = b.PAN")
  
  all.cust.5p$final_elig = all.cust.5p$Age_elg * (1-all.cust.5p$negative_pin) * all.cust.5p$servicable_flag * (1 - all.cust.5p$fiu_pan)
  sum(1-all.cust.5p$negative_pin)
  
  cust.bureau = all.cust.5p[all.cust.5p$final_elig ==1,]
  
  # write.xlsx(cust.bureau,'cust_to_bureau_20190207.xlsx',sheet = 'Sheet1')
  #nrow(cust.bureau)
  
#   cust.bureau$Created_On = as.Date(create.date,'%Y%m%d')
#   cust.bureau$Scrubbed_On = as.Date(NA)
# 
#   cols = c("ClientCode","FirstName","DateOfBirth","Mobile","PAN","Address", "Age","pincode",
#            "City_goi","servicable_flag", "fiu_pan","Age_elg","negative_pin",
#            "final_elig","Created_On","Scrubbed_On")
#   
#   data.upload = cust.bureau[,cols]
#   names(data.upload)[names(data.upload)=='FirstName'] = 'Name'
#   
#   data.export = data.upload[,c("ClientCode", "Name","DateOfBirth","Address","City_goi","pincode","PAN","Mobile")]
#   colnames(data.export) = c("Ac_Number","Name","DOB","Address","City","Pincode","Panno","Mobile")
#   write.csv(data.export, paste0('5Pcust_to_bureau_',create.date,'.csv'))
#   # write.csv(all.cust.5p, paste0('5Pcust_checked_',create.date,'.csv'))
# 
#   tab.name = paste0('Scrub_History_Cust5P_',create.date)
# 
# dbSendQuery(con_sandbox,paste0("create table ", tab.name,"(
#            ClientCode varchar(10)
#           ,Name varchar(100)
#           ,DateofBirth date
#           ,Mobile bigint
#           ,PAN varchar(10)
#           ,Address varchar(300)
#           ,Age int
#           ,Pincode int
#           ,City_goi varchar(50)
#           ,servicable_flag int
#           ,fiu_pan int
#           ,Age_elg int
#           ,negative_pin int
#           ,final_elig int
#           ,Created_On date
#           ,Scrubbed_on date)"))
# 
# dbWriteTable(con_sandbox, tab.name, data.upload,  append = F, overwrite = T,row.names=FALSE, encoding = "latin1" )

#########################################################################################################################################
#############################################################   Mobile location based   ###############################################################
#Select customers to be checked for mobile location
# all.cust.5p$mobile_location_2b_checked = all.cust.5p$Age_elg * (1-all.cust.5p$negative_pin) * (1-all.cust.5p$servicable_flag) * (1 - all.cust.5p$fiu_pan)
# 
# #Get mobile locations with high confidence for all customers possible
# Cust.Mobile.Based.Cities.HC <- get_mobile_based_location(create.date)
# 
# #Get customers falling serviceable cities
# mobile.serviceable.cities = c('ahmedabad'
#                               ,'delhi'
#                               ,'south delhi'
#                               ,'east delhi'
#                               ,'ghaziabad'
#                               ,'bangalore'
#                               ,'bangalore rural'
#                               ,'chennai'
#                               ,'coimbatore'
#                               ,'faridabad'
#                               ,'gurgaon'
#                               ,'hyderabad'
#                               ,'central delhi'
#                               ,'indore'
#                               ,'jaipur'
#                               ,'kolkata'
#                               ,'lucknow'
#                               ,'ludhiana'
#                               ,'mumbai'
#                               ,'pune'
#                               ,'thane'
#                               ,'south west delhi'
#                               ,'vadodara'
#                               ,'rajkot'
#                               ,'new delhi'
#                               ,'north delhi'
#                               ,'north east delhi'
#                               ,'north west delhi'
#                               ,'west delhi'
#                               ,'surat'
#                               ,'patna'
#                               ,'kanpur'
#                               ,'bhopal'
#                               ,'jodhpur'
#                               
#                               
#                               
# )
# '%!in%' <- function(x,y)!('%in%'(x,y))
# 
# 
# Customer.Mob.Loc.Comb <- merge(all.cust.5p,Cust.Mobile.Based.Cities.HC[,c("ClientCode","Most_Frequent_City")], by.x = "ClientCode", by.y = "ClientCode", all.x = T )
# 
# Customer.Mob.Loc.Comb$non_null_loc_flag <- ifelse(is.na(Customer.Mob.Loc.Comb$Most_Frequent_City) == F, 1,0 )
# Customer.Mob.Loc.Comb$servicable_flag_mob <- ifelse(tolower(Customer.Mob.Loc.Comb$Most_Frequent_City) %in% tolower(mobile.serviceable.cities)
#                                           & !is.na(Customer.Mob.Loc.Comb$Most_Frequent_City),1,0)
# 
# 
# 
# # cust.bureau.mob.loc = Customer.Mob.Loc.Comb[Customer.Mob.Loc.Comb$mobile_location_2b_checked ==1 & is.na(Customer.Mob.Loc.Comb$Most_Frequent_City) == F,]
# # write.xlsx(cust.bureau,'cust_to_bureau_20190207.xlsx',sheet = 'Sheet1')
# #nrow(cust.bureau)
# 
# Customer.Mob.Loc.Comb$Created_On = as.Date(create.date,'%Y%m%d')
# Customer.Mob.Loc.Comb$Scrubbed_On = as.Date(NA)
# Customer.Mob.Loc.Comb$final_elig_mob = (Customer.Mob.Loc.Comb$mobile_location_2b_checked) * (Customer.Mob.Loc.Comb$servicable_flag_mob) * (Customer.Mob.Loc.Comb$non_null_loc_flag)
# 
# cols = c("ClientCode","FirstName","DateOfBirth","Mobile","PAN","Address", "Age","pincode",
#          "City_goi","servicable_flag", "fiu_pan","Age_elg","negative_pin",
#          "final_elig","Created_On","Scrubbed_On")
# 
# data.upload.mob = Customer.Mob.Loc.Comb[Customer.Mob.Loc.Comb$final_elig_mob == 1,cols]
# names(data.upload.mob)[names(data.upload.mob)=='FirstName'] = 'Name'
# 
# data.export.mob = data.upload.mob[,c("ClientCode", "Name","DateOfBirth","Address","City_goi","pincode","PAN","Mobile")]
# colnames(data.export.mob) = c("Ac_Number","Name","DOB","Address","City","Pincode","Panno","Mobile")
# 
# write.csv(data.export.mob, paste0('5Pcust_to_bureau_Mob_Loc',create.date,'.csv'))
# # 
# tab.name.mob = paste0('Scrub_History_Cust5P_mob_loc',create.date)
# 
# dbSendQuery(con_sandbox,paste0("create table ", tab.name.mob,"(
#                                ClientCode varchar(10)
#                                ,Name varchar(100)
#                                ,DateofBirth date
#                                ,Mobile bigint
#                                ,PAN varchar(10)
#                                ,Address varchar(300)
#                                ,Age int
#                                ,Pincode int
#                                ,City_goi varchar(50)
#                                ,servicable_flag int
#                                ,fiu_pan int
#                                ,Age_elg int
#                                ,negative_pin int
#                                ,final_elig int
#                                ,Created_On date
#                                ,Scrubbed_on date)"))
# 
# dbWriteTable(con_sandbox, tab.name.mob, data.upload.mob,  append = F, overwrite = T,row.names=FALSE, encoding = "latin1" )

# end_time <- Sys.time()
# 
# time_diff = end_time - start_time
# time_diff
# Run below only for testing purposes 

#########################################################################################################################################
#############################################################   KYC location based  waterfall ###############################################################

level0 <- c( "All customers"
             , nrow(all.cust.5p)
             , nrow(all.cust.5p))

level1 <- c( "Age between 21 and 58"
             , nrow(all.cust.5p %>% filter(Age_elg == 1)  ) #Criteria1
             , nrow(all.cust.5p %>% filter(Age_elg == 1)  )) # criteria1

level2 <- c( "Non negative pincodes"
             , nrow(all.cust.5p %>% filter(negative_pin == 0)  ) #Criteria2
             , nrow(all.cust.5p %>% filter(Age_elg == 1
                                           & negative_pin == 0 )  )) #criteria1 and criteria2

level3 <- c( "No suit filed against"
             , nrow(all.cust.5p %>% filter(fiu_pan == 0)  ) #Criteria3
             , nrow(all.cust.5p %>% filter(Age_elg == 1
                                           & negative_pin == 0
                                           & fiu_pan == 0)  )) #criteria1, criteria2 and criteria 3

level4 <- c( "Serviceable KYC location"
             , nrow(all.cust.5p %>% filter(servicable_flag == 1)  ) #Criteria4
             , nrow(all.cust.5p %>% filter(Age_elg == 1
                                           & negative_pin == 0
                                           & fiu_pan == 0
                                           & servicable_flag == 1)  )) #criteria1, criteria2, criteria 3 and criteria4


waterfall_data <-  data.frame(t(data.frame( level0
                                 , level1
                                 , level2
                                 , level3
                                 , level4
                              
)))
colnames(waterfall_data) <- c("Criteria", "Decrease due to single criteria", "Remaining customers")

df = all.cust.5p %>% filter(Age_elg == 1
                            & negative_pin == 0
                            & fiu_pan == 0
                            & servicable_flag == 0)

nrow(df)
colnames(df)

req_df = sqldf("select a.City_goi, count(distinct ClientCode) as customers 
                from [df] a
                group by City_goi
              ")

write.csv(req_df, paste0('req_df.csv'))


write.csv(waterfall_data, paste0('Waterfall_Bureau_KYC_',create.date,'.csv'))


#########################################################################################################################################
#############################################################   Mobile location based  waterfall ###############################################################

level0 <- c( "All customers"
             , nrow(Customer.Mob.Loc.Comb)
             , nrow(Customer.Mob.Loc.Comb))

level1 <- c( "KYC non serviceable"
             , nrow(Customer.Mob.Loc.Comb %>% filter(mobile_location_2b_checked == 1)  ) #Criteria1
             , nrow(Customer.Mob.Loc.Comb %>% filter(mobile_location_2b_checked == 1)  )) # criteria1

level2 <- c( "Mobile address available"
             , nrow(Customer.Mob.Loc.Comb %>% filter(non_null_loc_flag == 1)  ) #Criteria2
             , nrow(Customer.Mob.Loc.Comb %>% filter(mobile_location_2b_checked == 1
                                           & non_null_loc_flag == 1 )  )) #criteria1 and criteria2

level3 <- c( "Mobile address serviceable"
             , nrow(Customer.Mob.Loc.Comb %>% filter(servicable_flag_mob == 1)  ) #Criteria3
             , nrow(Customer.Mob.Loc.Comb %>% filter(mobile_location_2b_checked == 1
                                           & non_null_loc_flag == 1
                                           & servicable_flag_mob == 1)  )) #criteria1, criteria2 and criteria 3


waterfall_data <-  data.frame(t(data.frame( level0
                                            , level1
                                            , level2
                                            , level3
                                            
)))
colnames(waterfall_data) <- c("Criteria", "Decrease due to single criteria", "Remaining customers")

write.csv(waterfall_data, paste0('Waterfall_Bureau_Mobile_',create.date,'.csv'))

#########################################################################################################################################
#############################################################   KYC location based  waterfall - New month ###############################################################
all.cust.5p.New.Month = all.cust.5p[all.cust.5p$New_month_cust ==1 
                                    # & is.na(all.cust.5p$Created_On) & as.Date(all.cust.5p$dateofjoining) < as.Date("2019-06-27", format = '%Y-%m-%d') & as.Date(all.cust.5p$dateofjoining) >= as.Date("2019-06-13", format = '%Y-%m-%d')
                                    , ]

level0 <- c( "All customers"
             , nrow(all.cust.5p.New.Month)
             , nrow(all.cust.5p.New.Month))

level1 <- c( "Age between 21 and 58"
             , nrow(all.cust.5p.New.Month %>% filter(Age_elg == 1)  ) #Criteria1
             , nrow(all.cust.5p.New.Month %>% filter(Age_elg == 1)  )) # criteria1

level2 <- c( "Non negative pincodes"
             , nrow(all.cust.5p.New.Month %>% filter(negative_pin == 0)  ) #Criteria2
             , nrow(all.cust.5p.New.Month %>% filter(Age_elg == 1
                                           & negative_pin == 0 )  )) #criteria1 and criteria2

level3 <- c( "No suit filed against"
             , nrow(all.cust.5p.New.Month %>% filter(fiu_pan == 0)  ) #Criteria3
             , nrow(all.cust.5p.New.Month %>% filter(Age_elg == 1
                                           & negative_pin == 0
                                           & fiu_pan == 0)  )) #criteria1, criteria2 and criteria 3

level4 <- c( "Serviceable KYC location"
             , nrow(all.cust.5p.New.Month %>% filter(servicable_flag == 1)  ) #Criteria4
             , nrow(all.cust.5p.New.Month %>% filter(Age_elg == 1
                                           & negative_pin == 0
                                           & fiu_pan == 0
                                           & servicable_flag == 1)  )) #criteria1, criteria2, criteria 3 and criteria4


waterfall_data <-  data.frame(t(data.frame( level0
                                            , level1
                                            , level2
                                            , level3
                                            , level4
                                            
)))
colnames(waterfall_data) <- c("Criteria", "Decrease due to single criteria", "Remaining customers")

write.csv(waterfall_data, paste0('Waterfall_Bureau_KYC_New_Month_non_repeat',create.date,'.csv'))

head(all.cust.5p.New.Month)

#########################################################################################################################################
#############################################################   ###############################################################


write.csv(all.cust.5p.New.Month, paste0('all.cust.5p.1st.to.27th.june.all',create.date,'.csv'))
#########################################################################################################################################
#############################################################   Testing   ###############################################################
#########################################################################################################################################


scrub.list.apr04.cc = as.data.frame(scrub.list.apr04$ClientCode)
scrub.list.mar26.cc = as.data.frame(scrub.list.mar26$ClientCode)

names(scrub.list.apr04.cc) = 'ClientCode'
names(scrub.list.mar26.cc) = 'ClientCode'

scrub.list.combined = rbind(scrub.list.apr04.cc,scrub.list.mar26.cc)


cust.bureau = sqldf("select a.*,
                     case when b.ClientCode is null then 1 else 0 end as refresh2
                     from [cust.bureau] a 
                     left join
                     [scrub.list.combined] b 
                     on a.ClientCode = b.ClientCode")



# Customers in the previous scrub list but not in the current list 

chck = sqldf("select a.*,
              case when b.ClientCode is null then 0 else 1 end as present_flag
              from
              [scrub.list.combined] a
              left join 
              [cust.bureau] b
              on a.ClientCode = b.ClientCode")

problem.cc = as.data.frame(chck$ClientCode[chck$present_flag==0])

names(problem.cc) = 'ClientCode'

# Re-run the bureau qualification criteria for these problematic codes

cust.5p  = filter(all.cust.5p, ClientCode %in% problem.cc$ClientCode) 



# Age Flag 

cust.5p$Age[!is.na(cust.5p$DateOfBirth)] = age_calc(dob = as.Date(cust.5p$DateOfBirth[!is.na(cust.5p$DateOfBirth)]), 
                                                            enddate = Sys.Date(), 
                                                            units = "years", precise = TRUE)

cust.5p$Age = as.integer(cust.5p$Age)

cust.5p$Age_elg = ifelse(cust.5p$Age>=21,ifelse(cust.5p$Age<=58,1,0),0)

# Negative pincode flag
cust.5p = sqldf("select a.*,
                    case when b.pincode is null then 0 else 1 end as 'negative_pin'
                    from [cust.5p] a 
                    left join [neg.pincode] b
                    on a.pincode = b.pincode")

# Update to servicable cities - April 2019
cust.5p = merge(cust.5p,pincode.data, by.x = 'pincode', by.y = 'pincode',  all.x=T)



#write.csv(servicable.cities, "servicable.cities.csv")

cust.5p$servicable_flag = ifelse(tolower(cust.5p$City_goi.x) %in% tolower(servicable.cities),1,0)


# FIU pan flag
cust.5p = sqldf("select a.*,
                    case when b.PAN is null then 0 else 1 end as 'fiu_pan'
                    from [cust.5p] a
                    left join [fiu.pan] b
                    on a.PAN = b.PAN")

cust.5p$final_elig = cust.5p$Age_elg * (1-cust.5p$negative_pin) * cust.5p$servicable_flag * (1 - cust.5p$fiu_pan)


#cust.bureau = all.cust.5p[all.cust.5p$final_elig ==1,]
# write.xlsx(cust.bureau,'cust_to_bureau_20190207.xlsx',sheet = 'Sheet1')
#nrow(cust.bureau)

cust.bureau$Created_On = as.Date(create.date,'%Y%m%d')
cust.bureau$Scrubbed_On = as.Date(NA)

DROP PROCEDURE CSMRT_OWNER.PS_D_EXT_ORG_P
/

--
-- PS_D_EXT_ORG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_EXT_ORG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_EXT_ORG from PeopleSoft tables PS_EXT_ORG_TBL, PS_EXT_ORG_TBL_ADM.
--
 --V01  SMT-xxxx 11/02/2017,    James Doucette
--                              Converted from DataStage
--V01.1 Case-44794 07/21/2020   JD Fixed SID lookup logic
--

--V01.2 Case-159563 04/06/2022  Smitha Paul added max Eff Date

--V01.3 Case-165244 06/02/2022  Smitha Paul added Inactive Flag
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_EXT_ORG';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_EXT_ORG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_EXT_ORG';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_EXT_ORG T                                                                                                                                                                                                  
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as ( 
select EXT_ORG_ID, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT EXT_ORG_SD, DESCR EXT_ORG_LD, DESCR50 EXT_ORG_FD,     -- Mar 2019 
       EXT_ORG_TYPE EXT_ORG_TYPE_ID, 
       ORG_LOCATION, PROPRIETORSHIP, 
       DATA_ORIGIN,  
       row_number() over (partition by EXT_ORG_ID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_EXT_ORG_TBL),
       Q2 as (
select EXT_ORG_ID, SRC_SYS_ID,EFFDT,
       LS_SCHOOL_TYPE SCHOOL_TYPE_ID, ACCREDITED ACCREDITED_FLG, ATP_CD, EXT_CAREER, EXT_TERM_TYPE, 
       OFFERS_COURSES OFFERS_COURSES_FLG, SHARED_CATALOG SHARED_CATALOG_FLG, UNT_TYPE, 
       row_number() over (partition by EXT_ORG_ID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_EXT_ORG_TBL_ADM
 where DATA_ORIGIN <> 'D'),
       Q3 as (
select LS_SCHOOL_TYPE SCHOOL_TYPE_ID, SRC_SYS_ID, 
       DESCRSHORT SCHOOL_TYPE_SD, DESCR SCHOOL_TYPE_LD, 
       row_number() over (partition by LS_SCHOOL_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_LS_SCHL_TYP_TBL
 where DATA_ORIGIN <> 'D'),
       Q4 as (
select EXT_ORG_ID, ORG_LOCATION, SRC_SYS_ID,  EFFDT,
       ADDRESS1 ADDR1_LD, ADDRESS2 ADDR2_LD, ADDRESS3 ADDR3_LD, ADDRESS4 ADDR4_LD, 
       CITY CITY_NM, COUNTY CNTY_NM, STATE STATE_ID, POSTAL POSTAL_CD, COUNTRY CNTRY_ID,  
       row_number() over (partition by EXT_ORG_ID, ORG_LOCATION, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ORG_LOCATION
 where DATA_ORIGIN <> 'D'),
       Q5 as (
select COUNTRY CNTRY_ID, STATE STATE_ID, SRC_SYS_ID, 
       DESCR STATE_LD
  from CSSTG_OWNER.PS_STATE_TBL
 where DATA_ORIGIN <> 'D'),
       Q6 as (
select COUNTRY CNTRY_ID, SRC_SYS_ID, 
       DESCRSHORT CNTRY_SD, DESCR CNTRY_LD
  from CSSTG_OWNER.PS_COUNTRY_TBL
 where DATA_ORIGIN <> 'D'),
       Q7 as (
select distinct 
        EXT_ORG_ID,
        ORG_GRP_TYPE, 
        ORG_GRP_CD, 
        SRC_SYS_ID,  'Y' AS INACTIVE_FLAG 
        from CSSTG_OWNER.PS_ORG_GROUPING
 where DATA_ORIGIN <> 'D'
 and ORG_GRP_TYPE = 'MNT' AND ORG_GRP_CD = 'INA'   
),
       S as (
select Q1.EXT_ORG_ID, Q1.SRC_SYS_ID, Q1.EFFDT, 
         Q2.EFFDT AS EFFDT2,Q4.EFFDT AS EFFDT3,  --April 2022
        Q1.EFF_STAT_CD, 
       Q1.EXT_ORG_SD, Q1.EXT_ORG_LD, Q1.EXT_ORG_FD, -- Mar 2019 
       Q1.EXT_ORG_TYPE_ID, 
       decode(Q1.EXT_ORG_TYPE_ID,'BUSN','Business','NONP','Non-Profit','OTHR','Other','SCHL','School','-') EXT_ORG_TYPE_SD,
       decode(Q1.EXT_ORG_TYPE_ID,'BUSN','Business','NONP','Non-Profit','OTHR','Other','SCHL','School','-') EXT_ORG_TYPE_LD,
       nvl(Q2.SCHOOL_TYPE_ID,'-') SCHOOL_TYPE_ID, nvl(Q3.SCHOOL_TYPE_SD,'-') SCHOOL_TYPE_SD, nvl(Q3.SCHOOL_TYPE_LD,'-') SCHOOL_TYPE_LD, 
       nvl(Q4.ADDR1_LD,'-') ADDR1_LD, nvl(Q4.ADDR2_LD,'-') ADDR2_LD, nvl(Q4.ADDR3_LD,'-') ADDR3_LD, nvl(Q4.ADDR4_LD,'-') ADDR4_LD, 
       nvl(Q4.CITY_NM,'-') CITY_NM, nvl(Q4.CNTY_NM,'-') CNTY_NM, 
       nvl(Q4.STATE_ID,'-') STATE_ID, nvl(Q5.STATE_LD,'-') STATE_LD, 
       nvl(Q4.POSTAL_CD,'-') POSTAL_CD, 
       nvl(Q4.CNTRY_ID,'-') CNTRY_ID, nvl(Q6.CNTRY_SD,'-') CNTRY_SD, nvl(Q6.CNTRY_LD,'-') CNTRY_LD, 
       nvl(Q2.ACCREDITED_FLG,'-') ACCREDITED_FLG, nvl(Q2.ATP_CD,'-') ATP_CD, 
       nvl(Q2.EXT_CAREER,'-') EXT_CAREER, nvl(X1.XLATSHORTNAME,'-') EXT_CAREER_SD, nvl(X1.XLATLONGNAME,'-') EXT_CAREER_LD,
       nvl(Q2.EXT_TERM_TYPE,'-') EXT_TERM_TYPE, nvl(X2.XLATSHORTNAME,'-') EXT_TERM_TYPE_SD, nvl(X2.XLATLONGNAME,'-') EXT_TERM_TYPE_LD,
       nvl(Q2.OFFERS_COURSES_FLG,'-') OFFERS_COURSES_FLG, 
       Q1.ORG_LOCATION, Q1.PROPRIETORSHIP, nvl(X3.XLATSHORTNAME,'-') PROPRIETORSHIP_SD, nvl(X3.XLATLONGNAME,'-') PROPRIETORSHIP_LD,
       nvl(Q2.SHARED_CATALOG_FLG,'-') SHARED_CATALOG_FLG, 
       nvl(Q2.UNT_TYPE,'-') UNT_TYPE, nvl(X4.XLATSHORTNAME,'-') UNT_TYPE_SD, nvl(X4.XLATLONGNAME,'-') UNT_TYPE_LD, 
       Q1.DATA_ORIGIN,
       NVL(Q7.INACTIVE_FLAG,'N')  AS INACTIVE_FLAG   
  from Q1
  left outer join Q2
    on Q1.EXT_ORG_ID = Q2.EXT_ORG_ID
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
  left outer join Q3
    on Q2.SCHOOL_TYPE_ID = Q3.SCHOOL_TYPE_ID
   and Q2.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  left outer join Q4
    on Q1.EXT_ORG_ID = Q4.EXT_ORG_ID
   and Q1.ORG_LOCATION = Q4.ORG_LOCATION
   and Q1.SRC_SYS_ID = Q4.SRC_SYS_ID
   and Q4.Q_ORDER = 1
  left outer join Q5
    on Q4.CNTRY_ID = Q5.CNTRY_ID
   and Q4.STATE_ID = Q5.STATE_ID
   and Q4.SRC_SYS_ID = Q5.SRC_SYS_ID
  left outer join Q6
    on Q4.CNTRY_ID = Q6.CNTRY_ID
   and Q4.SRC_SYS_ID = Q6.SRC_SYS_ID
  left outer join Q7
    on Q1.EXT_ORG_ID = Q7.EXT_ORG_ID
   and Q1.SRC_SYS_ID = Q7.SRC_SYS_ID
  left outer join X X1
    on Q2.EXT_CAREER = X1.FIELDVALUE
   and Q2.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'EXT_CAREER' 
   and X1.X_ORDER = 1  
  left outer join X X2
    on Q2.EXT_TERM_TYPE = X2.FIELDVALUE
   and Q2.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'EXT_TERM_TYPE' 
   and X2.X_ORDER = 1  
  left outer join X X3
    on Q1.PROPRIETORSHIP = X3.FIELDVALUE
   and Q1.SRC_SYS_ID = X3.SRC_SYS_ID
   and X3.FIELDNAME = 'PROPRIETORSHIP' 
   and X3.X_ORDER = 1  
  left outer join X X4
    on Q2.UNT_TYPE = X4.FIELDVALUE
   and Q2.SRC_SYS_ID = X4.SRC_SYS_ID
   and X4.FIELDNAME = 'UNT_TYPE' 
   and X4.X_ORDER = 1  
 where Q1.Q_ORDER = 1) 
--select nvl(D.EXT_ORG_SID, max(D.EXT_ORG_SID) over (partition by 1) +                                                                                                                                                                                            
--       row_number() over (partition by 1 order by D.EXT_ORG_SID nulls first)) EXT_ORG_SID, 
--New Logic:
select nvl(D.EXT_ORG_SID,
       (select nvl(max(EXT_ORG_SID),0) from CSMRT_OWNER.PS_D_EXT_ORG where EXT_ORG_SID <> 2147483646) +
        row_number() over (partition by 1 order by D.EXT_ORG_SID nulls first)) EXT_ORG_SID, -- July 2020
       nvl(D.EXT_ORG_ID, S.EXT_ORG_ID) EXT_ORG_ID,                                                                                                                                                                                                              
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       GREATEST(decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT),NVL(S.EFFDT2, to_date('19000101','YYYYMMDD')),NVL(S.EFFDT3, to_date('19000101','YYYYMMDD')))  as EFFDT,  --Apr 2022                                                                                                                                                                                                     
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.EXT_ORG_SD, S.EXT_ORG_SD, D.EXT_ORG_SD, S.EXT_ORG_SD) EXT_ORG_SD,                                                                                                                                                                               
       decode(D.EXT_ORG_LD, S.EXT_ORG_LD, D.EXT_ORG_LD, S.EXT_ORG_LD) EXT_ORG_LD,                                                                                                                                                                               
       decode(D.EXT_ORG_FD, S.EXT_ORG_FD, D.EXT_ORG_FD, S.EXT_ORG_FD) EXT_ORG_FD,                                                                                                                                                                               
       decode(D.EXT_ORG_TYPE_ID, S.EXT_ORG_TYPE_ID, D.EXT_ORG_TYPE_ID, S.EXT_ORG_TYPE_ID) EXT_ORG_TYPE_ID,                                                                                                                                                      
       decode(D.EXT_ORG_TYPE_SD, S.EXT_ORG_TYPE_SD, D.EXT_ORG_TYPE_SD, S.EXT_ORG_TYPE_SD) EXT_ORG_TYPE_SD,                                                                                                                                                      
       decode(D.EXT_ORG_TYPE_LD, S.EXT_ORG_TYPE_LD, D.EXT_ORG_TYPE_LD, S.EXT_ORG_TYPE_LD) EXT_ORG_TYPE_LD,                                                                                                                                                      
       decode(D.SCHOOL_TYPE_ID, S.SCHOOL_TYPE_ID, D.SCHOOL_TYPE_ID, S.SCHOOL_TYPE_ID) SCHOOL_TYPE_ID,                                                                                                                                                           
       decode(D.SCHOOL_TYPE_SD, S.SCHOOL_TYPE_SD, D.SCHOOL_TYPE_SD, S.SCHOOL_TYPE_SD) SCHOOL_TYPE_SD,                                                                                                                                                           
       decode(D.SCHOOL_TYPE_LD, S.SCHOOL_TYPE_LD, D.SCHOOL_TYPE_LD, S.SCHOOL_TYPE_LD) SCHOOL_TYPE_LD,                                                                                                                                                           
       decode(D.ADDR1_LD, S.ADDR1_LD, D.ADDR1_LD, S.ADDR1_LD) ADDR1_LD,                                                                                                                                                                                         
       decode(D.ADDR2_LD, S.ADDR2_LD, D.ADDR2_LD, S.ADDR2_LD) ADDR2_LD,                                                                                                                                                                                         
       decode(D.ADDR3_LD, S.ADDR3_LD, D.ADDR3_LD, S.ADDR3_LD) ADDR3_LD,                                                                                                                                                                                         
       decode(D.ADDR4_LD, S.ADDR4_LD, D.ADDR4_LD, S.ADDR4_LD) ADDR4_LD,                                                                                                                                                                                         
       decode(D.CITY_NM, S.CITY_NM, D.CITY_NM, S.CITY_NM) CITY_NM,                                                                                                                                                                                              
       decode(D.CNTY_NM, S.CNTY_NM, D.CNTY_NM, S.CNTY_NM) CNTY_NM,                                                                                                                                                                                              
       decode(D.STATE_ID, S.STATE_ID, D.STATE_ID, S.STATE_ID) STATE_ID,                                                                                                                                                                                         
       decode(D.STATE_LD, S.STATE_LD, D.STATE_LD, S.STATE_LD) STATE_LD,                                                                                                                                                                                         
       decode(D.POSTAL_CD, S.POSTAL_CD, D.POSTAL_CD, S.POSTAL_CD) POSTAL_CD,                                                                                                                                                                                    
       decode(D.CNTRY_ID, S.CNTRY_ID, D.CNTRY_ID, S.CNTRY_ID) CNTRY_ID,                                                                                                                                                                                         
       decode(D.CNTRY_SD, S.CNTRY_SD, D.CNTRY_SD, S.CNTRY_SD) CNTRY_SD,                                                                                                                                                                                         
       decode(D.CNTRY_LD, S.CNTRY_LD, D.CNTRY_LD, S.CNTRY_LD) CNTRY_LD,                                                                                                                                                                                         
       decode(D.ACCREDITED_FLG, S.ACCREDITED_FLG, D.ACCREDITED_FLG, S.ACCREDITED_FLG) ACCREDITED_FLG,                                                                                                                                                           
       decode(D.ATP_CD, S.ATP_CD, D.ATP_CD, S.ATP_CD) ATP_CD,                                                                                                                                                                                                   
       decode(D.EXT_CAREER, S.EXT_CAREER, D.EXT_CAREER, S.EXT_CAREER) EXT_CAREER,                                                                                                                                                                               
       decode(D.EXT_CAREER_SD, S.EXT_CAREER_SD, D.EXT_CAREER_SD, S.EXT_CAREER_SD) EXT_CAREER_SD,                                                                                                                                                                
       decode(D.EXT_CAREER_LD, S.EXT_CAREER_LD, D.EXT_CAREER_LD, S.EXT_CAREER_LD) EXT_CAREER_LD,                                                                                                                                                                
       decode(D.EXT_TERM_TYPE, S.EXT_TERM_TYPE, D.EXT_TERM_TYPE, S.EXT_TERM_TYPE) EXT_TERM_TYPE,                                                                                                                                                                
       decode(D.EXT_TERM_TYPE_SD, S.EXT_TERM_TYPE_SD, D.EXT_TERM_TYPE_SD, S.EXT_TERM_TYPE_SD) EXT_TERM_TYPE_SD,                                                                                                                                                 
       decode(D.EXT_TERM_TYPE_LD, S.EXT_TERM_TYPE_LD, D.EXT_TERM_TYPE_LD, S.EXT_TERM_TYPE_LD) EXT_TERM_TYPE_LD,                                                                                                                                                 
       decode(D.OFFERS_COURSES_FLG, S.OFFERS_COURSES_FLG, D.OFFERS_COURSES_FLG, S.OFFERS_COURSES_FLG) OFFERS_COURSES_FLG,                                                                                                                                       
       decode(D.ORG_LOCATION, S.ORG_LOCATION, D.ORG_LOCATION, S.ORG_LOCATION) ORG_LOCATION,                                                                                                                                                                     
       decode(D.PROPRIETORSHIP, S.PROPRIETORSHIP, D.PROPRIETORSHIP, S.PROPRIETORSHIP) PROPRIETORSHIP,                                                                                                                                                           
       decode(D.PROPRIETORSHIP_SD, S.PROPRIETORSHIP_SD, D.PROPRIETORSHIP_SD, S.PROPRIETORSHIP_SD) PROPRIETORSHIP_SD,                                                                                                                                            
       decode(D.PROPRIETORSHIP_LD, S.PROPRIETORSHIP_LD, D.PROPRIETORSHIP_LD, S.PROPRIETORSHIP_LD) PROPRIETORSHIP_LD,                                                                                                                                            
       decode(D.SHARED_CATALOG_FLG, S.SHARED_CATALOG_FLG, D.SHARED_CATALOG_FLG, S.SHARED_CATALOG_FLG) SHARED_CATALOG_FLG,                                                                                                                                       
       decode(D.UNT_TYPE, S.UNT_TYPE, D.UNT_TYPE, S.UNT_TYPE) UNT_TYPE,                                                                                                                                                                                         
       decode(D.UNT_TYPE_SD, S.UNT_TYPE_SD, D.UNT_TYPE_SD, S.UNT_TYPE_SD) UNT_TYPE_SD,                                                                                                                                                                                                   
       decode(D.UNT_TYPE_LD, S.UNT_TYPE_LD, D.UNT_TYPE_LD, S.UNT_TYPE_LD) UNT_TYPE_LD,                                                                                                                                                                                                   
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM,
       S.INACTIVE_FLAG AS INACTIVE_FLAG                                                                                                                                                                                                               
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_EXT_ORG D                                                                                                                                                                                                                
    on D.EXT_ORG_SID <> 2147483646                                                                                                                                                                                                                              
   and D.EXT_ORG_ID = S.EXT_ORG_ID                                                                                                                                                                                                                              
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.EXT_ORG_ID = S.EXT_ORG_ID                                                                                                                                                                                                                            
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.EXT_ORG_SD = S.EXT_ORG_SD,                                                                                                                                                                                                                             
       T.EXT_ORG_LD = S.EXT_ORG_LD,                                                                                                                                                                                                                             
       T.EXT_ORG_FD = S.EXT_ORG_FD,                                                                                                                                                                                                                             
       T.EXT_ORG_TYPE_ID = S.EXT_ORG_TYPE_ID,                                                                                                                                                                                                                   
       T.EXT_ORG_TYPE_SD = S.EXT_ORG_TYPE_SD,                                                                                                                                                                                                                   
       T.EXT_ORG_TYPE_LD = S.EXT_ORG_TYPE_LD,                                                                                                                                                                                                                   
       T.SCHOOL_TYPE_ID = S.SCHOOL_TYPE_ID,                                                                                                                                                                                                                     
       T.SCHOOL_TYPE_SD = S.SCHOOL_TYPE_SD,                                                                                                                                                                                                                     
       T.SCHOOL_TYPE_LD = S.SCHOOL_TYPE_LD,                                                                                                                                                                                                                     
       T.ADDR1_LD = S.ADDR1_LD,                                                                                                                                                                                                                                 
       T.ADDR2_LD = S.ADDR2_LD,                                                                                                                                                                                                                                 
       T.ADDR3_LD = S.ADDR3_LD,                                                                                                                                                                                                                                 
       T.ADDR4_LD = S.ADDR4_LD,                                                                                                                                                                                                                                 
       T.CITY_NM = S.CITY_NM,                                                                                                                                                                                                                                   
       T.CNTY_NM = S.CNTY_NM,                                                                                                                                                                                                                                   
       T.STATE_ID = S.STATE_ID,                                                                                                                                                                                                                                 
       T.STATE_LD = S.STATE_LD,                                                                                                                                                                                                                                 
       T.POSTAL_CD = S.POSTAL_CD,                                                                                                                                                                                                                               
       T.CNTRY_ID = S.CNTRY_ID,                                                                                                                                                                                                                                 
       T.CNTRY_SD = S.CNTRY_SD,                                                                                                                                                                                                                                 
       T.CNTRY_LD = S.CNTRY_LD,                                                                                                                                                                                                                                 
       T.ACCREDITED_FLG = S.ACCREDITED_FLG,                                                                                                                                                                                                                     
       T.ATP_CD = S.ATP_CD,                                                                                                                                                                                                                                     
       T.EXT_CAREER = S.EXT_CAREER,                                                                                                                                                                                                                             
       T.EXT_CAREER_SD = S.EXT_CAREER_SD,                                                                                                                                                                                                                       
       T.EXT_CAREER_LD = S.EXT_CAREER_LD,                                                                                                                                                                                                                       
       T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,                                                                                                                                                                                                                       
       T.EXT_TERM_TYPE_SD = S.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                 
       T.EXT_TERM_TYPE_LD = S.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                 
       T.OFFERS_COURSES_FLG = S.OFFERS_COURSES_FLG,                                                                                                                                                                                                             
       T.ORG_LOCATION = S.ORG_LOCATION,                                                                                                                                                                                                                         
       T.PROPRIETORSHIP = S.PROPRIETORSHIP,                                                                                                                                                                                                                     
       T.PROPRIETORSHIP_SD = S.PROPRIETORSHIP_SD,                                                                                                                                                                                                               
       T.PROPRIETORSHIP_LD = S.PROPRIETORSHIP_LD,                                                                                                                                                                                                               
       T.SHARED_CATALOG_FLG = S.SHARED_CATALOG_FLG,                                                                                                                                                                                                             
       T.UNT_TYPE = S.UNT_TYPE,                                                                                                                                                                                                                                 
       T.UNT_TYPE_SD = S.UNT_TYPE_SD,                                                                                                                                                                                                                                     
       T.UNT_TYPE_LD = S.UNT_TYPE_LD,                                                                                                                                                                                                                                     
       T.DATA_ORIGIN = S.DATA_ORIGIN,   
       T.INACTIVE_FLAG = S.INACTIVE_FLAG,                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.EXT_ORG_SD,S.EXT_ORG_SD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_ORG_LD,S.EXT_ORG_LD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_ORG_FD,S.EXT_ORG_FD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_ORG_TYPE_ID,S.EXT_ORG_TYPE_ID,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.EXT_ORG_TYPE_SD,S.EXT_ORG_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.EXT_ORG_TYPE_LD,S.EXT_ORG_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.SCHOOL_TYPE_ID,S.SCHOOL_TYPE_ID,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.SCHOOL_TYPE_SD,S.SCHOOL_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.SCHOOL_TYPE_LD,S.SCHOOL_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.ADDR1_LD,S.ADDR1_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR2_LD,S.ADDR2_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR3_LD,S.ADDR3_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR4_LD,S.ADDR4_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.CITY_NM,S.CITY_NM,0,1) = 1 or                                                                                                                                                                                                                   
       decode(T.CNTY_NM,S.CNTY_NM,0,1) = 1 or                                                                                                                                                                                                                   
       decode(T.STATE_ID,S.STATE_ID,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.STATE_LD,S.STATE_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.POSTAL_CD,S.POSTAL_CD,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.CNTRY_ID,S.CNTRY_ID,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.CNTRY_SD,S.CNTRY_SD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.CNTRY_LD,S.CNTRY_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ACCREDITED_FLG,S.ACCREDITED_FLG,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.ATP_CD,S.ATP_CD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.EXT_CAREER,S.EXT_CAREER,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_CAREER_SD,S.EXT_CAREER_SD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.EXT_CAREER_LD,S.EXT_CAREER_LD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.EXT_TERM_TYPE,S.EXT_TERM_TYPE,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.EXT_TERM_TYPE_SD,S.EXT_TERM_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.EXT_TERM_TYPE_LD,S.EXT_TERM_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.OFFERS_COURSES_FLG,S.OFFERS_COURSES_FLG,0,1) = 1 or                                                                                                                                                                                             
       decode(T.ORG_LOCATION,S.ORG_LOCATION,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.PROPRIETORSHIP,S.PROPRIETORSHIP,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.PROPRIETORSHIP_SD,S.PROPRIETORSHIP_SD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.PROPRIETORSHIP_LD,S.PROPRIETORSHIP_LD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.SHARED_CATALOG_FLG,S.SHARED_CATALOG_FLG,0,1) = 1 or                                                                                                                                                                                             
       decode(T.UNT_TYPE,S.UNT_TYPE,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.UNT_TYPE_SD,S.UNT_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.UNT_TYPE_LD,S.UNT_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1   or
       decode(T.INACTIVE_FLAG,S.INACTIVE_FLAG,0,1) = 1                                                                                                                                                                                                            
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.EXT_ORG_SID,                                                                                                                                                                                                                                           
       T.EXT_ORG_ID,                                                                                                                                                                                                                                            
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.EXT_ORG_SD,                                                                                                                                                                                                                                            
       T.EXT_ORG_LD,                                                                                                                                                                                                                                            
       T.EXT_ORG_FD,                                                                                                                                                                                                                                            
       T.EXT_ORG_TYPE_ID,                                                                                                                                                                                                                                       
       T.EXT_ORG_TYPE_SD,                                                                                                                                                                                                                                       
       T.EXT_ORG_TYPE_LD,                                                                                                                                                                                                                                       
       T.SCHOOL_TYPE_ID,                                                                                                                                                                                                                                        
       T.SCHOOL_TYPE_SD,                                                                                                                                                                                                                                        
       T.SCHOOL_TYPE_LD,                                                                                                                                                                                                                                        
       T.ADDR1_LD,                                                                                                                                                                                                                                              
       T.ADDR2_LD,                                                                                                                                                                                                                                              
       T.ADDR3_LD,                                                                                                                                                                                                                                              
       T.ADDR4_LD,                                                                                                                                                                                                                                              
       T.CITY_NM,                                                                                                                                                                                                                                               
       T.CNTY_NM,                                                                                                                                                                                                                                               
       T.STATE_ID,                                                                                                                                                                                                                                              
       T.STATE_LD,                                                                                                                                                                                                                                              
       T.POSTAL_CD,                                                                                                                                                                                                                                             
       T.CNTRY_ID,                                                                                                                                                                                                                                              
       T.CNTRY_SD,                                                                                                                                                                                                                                              
       T.CNTRY_LD,                                                                                                                                                                                                                                              
       T.ACCREDITED_FLG,                                                                                                                                                                                                                                        
       T.ATP_CD,                                                                                                                                                                                                                                                
       T.EXT_CAREER,                                                                                                                                                                                                                                            
       T.EXT_CAREER_SD,                                                                                                                                                                                                                                         
       T.EXT_CAREER_LD,                                                                                                                                                                                                                                         
       T.EXT_TERM_TYPE,                                                                                                                                                                                                                                         
       T.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                                      
       T.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                                      
       T.OFFERS_COURSES_FLG,                                                                                                                                                                                                                                    
       T.ORG_LOCATION,                                                                                                                                                                                                                                          
       T.PROPRIETORSHIP,                                                                                                                                                                                                                                        
       T.PROPRIETORSHIP_SD,                                                                                                                                                                                                                                     
       T.PROPRIETORSHIP_LD,                                                                                                                                                                                                                                     
       T.SHARED_CATALOG_FLG,                                                                                                                                                                                                                                    
       T.UNT_TYPE,                                                                                                                                                                                                                                              
       T.UNT_TYPE_SD,                                                                                                                                                                                                                                                
       T.UNT_TYPE_LD,                                                                                                                                                                                                                                                
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM,
       T.INACTIVE_FLAG)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.EXT_ORG_SID,                                                                                                                                                                                                                                           
       S.EXT_ORG_ID,                                                                                                                                                                                                                                            
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.EXT_ORG_SD,                                                                                                                                                                                                                                            
       S.EXT_ORG_LD,                                                                                                                                                                                                                                            
       S.EXT_ORG_FD,                                                                                                                                                                                                                                            
       S.EXT_ORG_TYPE_ID,                                                                                                                                                                                                                                       
       S.EXT_ORG_TYPE_SD,                                                                                                                                                                                                                                       
       S.EXT_ORG_TYPE_LD,                                                                                                                                                                                                                                       
       S.SCHOOL_TYPE_ID,                                                                                                                                                                                                                                        
       S.SCHOOL_TYPE_SD,                                                                                                                                                                                                                                        
       S.SCHOOL_TYPE_LD,                                                                                                                                                                                                                                        
       S.ADDR1_LD,                                                                                                                                                                                                                                              
       S.ADDR2_LD,                                                                                                                                                                                                                                              
       S.ADDR3_LD,                                                                                                                                                                                                                                              
       S.ADDR4_LD,                                                                                                                                                                                                                                              
       S.CITY_NM,                                                                                                                                                                                                                                               
       S.CNTY_NM,                                                                                                                                                                                                                                               
       S.STATE_ID,                                                                                                                                                                                                                                              
       S.STATE_LD,                                                                                                                                                                                                                                              
       S.POSTAL_CD,                                                                                                                                                                                                                                             
       S.CNTRY_ID,                                                                                                                                                                                                                                              
       S.CNTRY_SD,                                                                                                                                                                                                                                              
       S.CNTRY_LD,                                                                                                                                                                                                                                              
       S.ACCREDITED_FLG,                                                                                                                                                                                                                                        
       S.ATP_CD,                                                                                                                                                                                                                                                
       S.EXT_CAREER,                                                                                                                                                                                                                                            
       S.EXT_CAREER_SD,                                                                                                                                                                                                                                         
       S.EXT_CAREER_LD,                                                                                                                                                                                                                                         
       S.EXT_TERM_TYPE,                                                                                                                                                                                                                                         
       S.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                                      
       S.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                                      
       S.OFFERS_COURSES_FLG,                                                                                                                                                                                                                                    
       S.ORG_LOCATION,                                                                                                                                                                                                                                          
       S.PROPRIETORSHIP,                                                                                                                                                                                                                                        
       S.PROPRIETORSHIP_SD,                                                                                                                                                                                                                                     
       S.PROPRIETORSHIP_LD,                                                                                                                                                                                                                                     
       S.SHARED_CATALOG_FLG,                                                                                                                                                                                                                                    
       S.UNT_TYPE,                                                                                                                                                                                                                                              
       S.UNT_TYPE_SD,                                                                                                                                                                                                                                                
       S.UNT_TYPE_LD,                                                                                                                                                                                                                                                
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE,
       S.INACTIVE_FLAG)
;                                                                                                                                                                                                                                                    

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_ORG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_ORG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_EXT_ORG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_EXT_ORG';
update CSMRT_OWNER.PS_D_EXT_ORG T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.EXT_ORG_SID < 2147483646
   and not exists (select 1 
                     from CSSTG_OWNER.PS_EXT_ORG_TBL S
                    where T.EXT_ORG_ID = S.EXT_ORG_ID
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_ORG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_ORG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_D_EXT_ORG_P;
/

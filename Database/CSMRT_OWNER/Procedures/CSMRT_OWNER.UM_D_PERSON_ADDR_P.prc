CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_ADDR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_D_PERSON_ADDR
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_ADDR';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_ADDR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_ADDR');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_ADDR disable constraint PK_UM_D_PERSON_ADDR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_ADDR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_ADDR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_ADDR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_ADDR';				
insert /*+ append */ into CSMRT_OWNER.UM_D_PERSON_ADDR
with X as (  
select /*+ inline parallel(16) */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ inline parallel(16) */
       A.EMPLID PERSON_ID, A.ADDRESS_TYPE, A.EFFDT, A.SRC_SYS_ID,
       A.EFF_STATUS, 
       nvl(X1.XLATSHORTNAME,'-') ADDRESS_TYPE_SD, nvl(X1.XLATLONGNAME,'-') ADDRESS_TYPE_LD, 
       decode(A.ADDRESS1,'-','',A.ADDRESS1) ADDRESS1, decode(A.ADDRESS2,'-','',A.ADDRESS2) ADDRESS2, decode(A.ADDRESS3,'-','',A.ADDRESS3) ADDRESS3, decode(A.ADDRESS4,'-','',A.ADDRESS4) ADDRESS4, 
       decode(A.CITY,'-','',A.CITY) CITY, decode(A.COUNTY,'-','',A.COUNTY) COUNTY, decode(A.STATE,'-','',A.STATE) STATE, decode(S.DESCR,'-','',S.DESCR) STATE_LD, 
       decode(A.POSTAL,'-','',A.POSTAL) POSTAL, 
       case when A.COUNTRY = 'USA' and A.POSTAL <> '-' then substr(A.POSTAL, 1, 3) else '' end POSTAL3_USA_CD, 
       case when A.COUNTRY = 'USA' and A.POSTAL <> '-' then substr(A.POSTAL, 1, 5) else '' end POSTAL5_USA_CD, 
       case when A.COUNTRY = 'USA' and A.POSTAL <> '-' and length(A.POSTAL) = 10 then substr(A.POSTAL, -4, 4) else '' end POSTAL_PLUS4_USA_CD,           
          (CASE WHEN  A.STATE = 'NH'
                 AND (A.POSTAL LIKE '03033%'
                   OR A.POSTAL LIKE '03034%'
                   OR A.POSTAL LIKE '03038%'
                   OR A.POSTAL LIKE '03041%'
                   OR A.POSTAL LIKE '03049%'
                   OR A.POSTAL LIKE '03051%'
                   OR A.POSTAL LIKE '03052%'
                   OR A.POSTAL LIKE '03053%'
                   OR A.POSTAL LIKE '03054%'
                   OR A.POSTAL LIKE '03060%'
                   OR A.POSTAL LIKE '03061%'
                   OR A.POSTAL LIKE '03062%'
                   OR A.POSTAL LIKE '03063%'
                   OR A.POSTAL LIKE '03064%'
                   OR A.POSTAL LIKE '03073%'
                   OR A.POSTAL LIKE '03076%'
                   OR A.POSTAL LIKE '03079%'
                   OR (A.POSTAL LIKE '03106%' and upper(A.CITY) <> 'HOOKSETT') 
                   OR A.POSTAL LIKE '03811%'
                   OR A.POSTAL LIKE '03826%'
                   OR A.POSTAL LIKE '03841%'
                   OR A.POSTAL LIKE '03865%')
              THEN 'NOPLAN'
              ELSE '-' END) UMLOW_GRAD_PROXIMITY,
          (CASE WHEN  A.STATE = 'NH'
                 AND (A.POSTAL LIKE '03033%'
                   OR A.POSTAL LIKE '03049%'
                   OR A.POSTAL LIKE '03051%'
                   OR A.POSTAL LIKE '03060%'
                   OR A.POSTAL LIKE '03061%'
                   OR A.POSTAL LIKE '03062%'
                   OR A.POSTAL LIKE '03063%'
                   OR A.POSTAL LIKE '03064%'
                   OR A.POSTAL LIKE '03073%'
                   OR A.POSTAL LIKE '03076%'
                   OR A.POSTAL LIKE '03079%'
                   OR A.POSTAL LIKE '03087%'
                   OR A.POSTAL LIKE '03811%'
                   OR A.POSTAL LIKE '03865%')
                THEN 'NOPLAN'
                WHEN  A.STATE = 'NH'
                 AND (A.POSTAL LIKE '03031%'
                   OR A.POSTAL LIKE '03032%'
                   OR A.POSTAL LIKE '03036%'
                   OR A.POSTAL LIKE '03038%'
                   OR A.POSTAL LIKE '03041%'
                   OR A.POSTAL LIKE '03045%'
                   OR A.POSTAL LIKE '03048%'    
                   OR A.POSTAL LIKE '03052%'
                   OR A.POSTAL LIKE '03053%'
                   OR A.POSTAL LIKE '03054%'
                   OR A.POSTAL LIKE '03055%'
                   OR A.POSTAL LIKE '03057%'
                   OR A.POSTAL LIKE '03070%'
                   OR A.POSTAL LIKE '03082%'
                   OR A.POSTAL LIKE '03086%'
                   OR A.POSTAL LIKE '03101%'
                   OR A.POSTAL LIKE '03102%'
                   OR A.POSTAL LIKE '03103%'
                   OR A.POSTAL LIKE '03104%'
                   OR A.POSTAL LIKE '03105%'
--                   OR A.POSTAL LIKE '03106%'
                   OR (A.POSTAL LIKE '03106%' and upper(A.CITY) <> 'HOOKSETT') 
                   OR A.POSTAL LIKE '03107%'
                   OR A.POSTAL LIKE '03108%'
                   OR A.POSTAL LIKE '03109%'
                   OR A.POSTAL LIKE '03110%'
                   OR A.POSTAL LIKE '03111%'
                   OR A.POSTAL LIKE '03819%'
                   OR A.POSTAL LIKE '03826%'
                   OR A.POSTAL LIKE '03841%'
                   OR A.POSTAL LIKE '03873%')
              THEN 'PLAN'
              ELSE '-' END) UMLOW_UGRD_PROXIMITY,
       decode(A.COUNTRY,'-','',A.COUNTRY) COUNTRY, decode(C.DESCRSHORT,'-','',C.DESCRSHORT) COUNTRY_SD, decode(C.DESCR,'-','',C.DESCR) COUNTRY_LD, decode(C.COUNTRY_2CHAR,'-','',C.COUNTRY_2CHAR) COUNTRY_2CHAR, decode(C.EU_MEMBER_STATE,'-','',C.EU_MEMBER_STATE) EU_MEMBER_STATE, 
       A.LASTUPDDTTM, A.LASTUPDOPRID,
       row_number() over (partition by A.EMPLID, A.ADDRESS_TYPE, A.SRC_SYS_ID
                              order by (case when A.EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else A.EFFDT end) desc) Q_ORDER,
       row_number() over (partition by A.EMPLID, A.ADDRESS_TYPE, A.SRC_SYS_ID
                              order by decode(EFF_STATUS,'A',0,9), (case when A.EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else A.EFFDT end) desc) Q_ORDER2,
       A.DATA_ORIGIN
  from CSSTG_OWNER.PS_ADDRESSES A
  left outer join CSSTG_OWNER.PS_COUNTRY_TBL C
    on A.COUNTRY = C.COUNTRY 
   and A.SRC_SYS_ID = C.SRC_SYS_ID
   and C.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_STATE_TBL S
    on A.COUNTRY = S.COUNTRY 
   and A.STATE = S.STATE
   and A.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
  left outer join X X1
    on A.ADDRESS_TYPE = X1.FIELDVALUE
   and A.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'ADDR_TYPE1'
   and X1.X_ORDER = 1
 where A.DATA_ORIGIN <> 'D'
   and A.EFFDT <= trunc(SYSDATE)    -- Aug 2019 
 union all
select /*+ /*+ inline parallel(16) */ 
       PERSON_ID, '-' ADDRESS_TYPE, TO_DATE('01/01/1900', 'MM/DD/YYYY') EFFDT, SRC_SYS_ID,
       'A' EFF_STATUS, '' ADDRESS_TYPE_SD, '' ADDRESS_TYPE_LD, 
       '' ADDRESS1, '' ADDRESS2, '' ADDRESS3, '' ADDRESS4, '' CITY, '' COUNTY, '' STATE, '' STATE_LD, 
       '' POSTAL, '' POSTAL3_USA_CD, '' POSTAL5_USA_CD, '' POSTAL_PLUS4_USA_CD, 
       '' UMLOW_GRAD_PROXIMITY, '' UMLOW_UGRD_PROXIMITY, 
       '' COUNTRY, '' COUNTRY_SD, '' COUNTRY_LD, '' COUNTRY_2CHAR, '' EU_MEMBER_STATE,
       SYSDATE AS LASTUPDDTTM, '-' LASTUPDOPRID, 1 Q_ORDER, 1 Q_ORDER2, 'S' DATA_ORIGIN
  from PS_D_PERSON
 where DATA_ORIGIN <> 'D'),
       Q2 as (  
select /*+ inline parallel(16) */
       PERSON_ID, ADDRESS_TYPE, EFFDT, SRC_SYS_ID, 
       decode(max(EFFDT) over (partition by PERSON_ID, ADDRESS_TYPE, SRC_SYS_ID
                                   order by EFFDT 
                               rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),EFFDT) EFFDT_START,   
       nvl(min(EFFDT-1) over (partition by PERSON_ID, ADDRESS_TYPE, SRC_SYS_ID
                                  order by EFFDT
                              rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,      -- Added  
       row_number() over (partition by PERSON_ID, ADDRESS_TYPE, SRC_SYS_ID
                              order by EFFDT desc) EFFDT_ORDER,                                                         -- Added 
       EFF_STATUS, ADDRESS_TYPE_SD, ADDRESS_TYPE_LD, 
       ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4, CITY, COUNTY, 
       STATE, STATE_LD, POSTAL, POSTAL3_USA_CD, POSTAL5_USA_CD, POSTAL_PLUS4_USA_CD, 
       UMLOW_GRAD_PROXIMITY, UMLOW_UGRD_PROXIMITY, COUNTRY, COUNTRY_SD, COUNTRY_LD, COUNTRY_2CHAR, EU_MEMBER_STATE,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'BILL',0,'MAIL',1,'LOCL',2,'PERM',3,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) BMLP_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'MAIL',0,'LOCL',1,'PERM',2,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) MLP_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'MAIL',0,'PERM',1,'LOCL',2,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) MPL_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'PERM',0,'MAIL',1,'LOCL',2,'RESH',9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) PML_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'DIPL',0,'MAIL',1,'LOCL',2,'PERM',3,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) DMLP_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'DIPL',0,'MAIL',1,'PERM',2,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) DMP_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'DIPL',0,'PERM',1,'MAIL',2,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) DPM_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'PERM',0,'-',1,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) PERM_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'MAIL',0,'-',1,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) MAIL_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), decode(ADDRESS_TYPE,'RESH',0,'-',1,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) RESH_ADDR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), 
                                       case when ADDRESS_TYPE = 'RESH' and upper(CITY) like '%BOSTON%' then 0 
                                            when ADDRESS_TYPE = '-' then 1
                                            else 9 
                                        end, Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) RESH_UMBOS_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), 
                                       case when ADDRESS_TYPE = 'RESH' and upper(CITY) like '%DARTMOUTH%' then 0 
                                            when ADDRESS_TYPE = '-' then 1
                                            else 9 
                                        end, decode(EFF_STATUS,'A',0,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) RESH_UMDAR_ORDER,
       row_number() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(Q_ORDER,Q_ORDER2,0,9), decode(EFF_STATUS,'A',0,9), 
                                       case when ADDRESS_TYPE = 'RESH' and upper(CITY) like '%LOWELL%' then 0 
                                            when ADDRESS_TYPE = '-' then 1
                                            else 9 
                                        end, decode(EFF_STATUS,'A',0,9), Q_ORDER2, ADDRESS_TYPE desc, EFFDT desc) RESH_UMLOW_ORDER,
       dense_rank() over (partition by PERSON_ID, SRC_SYS_ID
                              order by decode(ADDRESS_TYPE,'PERM',0,'MAIL',1,'LOCL',2,'RESH',3,9), ADDRESS_TYPE desc, EFFDT desc) PML_HIST_ORDER,
       Q_ORDER ADDR_ORDER, LASTUPDDTTM, LASTUPDOPRID,
       Q1.DATA_ORIGIN
  from Q1)
select /*+ inline parallel(16) */
       P.PERSON_ID, nvl(ADDRESS_TYPE,'-') ADDRESS_TYPE, nvl(EFFDT,to_date('01-JAN-1900')) EFFDT, P.SRC_SYS_ID, 
       nvl(EFFDT_START,to_date('01-JAN-1800')) EFFDT_START, nvl(EFFDT_END,to_date('31-DEC-9999')) EFFDT_END, nvl(EFFDT_ORDER,1) EFFDT_ORDER, nvl(EFF_STATUS,'A') EFF_STATUS, 
       P.PERSON_SID, 
       nvl(ADDRESS_TYPE_SD,'-') ADDRESS_TYPE_SD, nvl(ADDRESS_TYPE_LD,'-') ADDRESS_TYPE_LD, 
       ADDRESS1, ADDRESS2, ADDRESS3, ADDRESS4, CITY, COUNTY, 
       STATE, STATE_LD, POSTAL, POSTAL3_USA_CD, POSTAL5_USA_CD, POSTAL_PLUS4_USA_CD, 
       nvl(UMLOW_GRAD_PROXIMITY,'-') UMLOW_GRAD_PROXIMITY, nvl(UMLOW_UGRD_PROXIMITY,'-') UMLOW_UGRD_PROXIMITY, COUNTRY, COUNTRY_SD, COUNTRY_LD, COUNTRY_2CHAR, EU_MEMBER_STATE,
       nvl(BMLP_ADDR_ORDER,1) BMLP_ADDR_ORDER, nvl(MLP_ADDR_ORDER,1) MLP_ADDR_ORDER, nvl(MPL_ADDR_ORDER,1) MPL_ADDR_ORDER, nvl(PML_ADDR_ORDER,1) PML_ADDR_ORDER, 
       nvl(DMLP_ADDR_ORDER,1) DMLP_ADDR_ORDER, nvl(DMP_ADDR_ORDER,1) DMP_ADDR_ORDER, nvl(DPM_ADDR_ORDER,1) DPM_ADDR_ORDER, nvl(PERM_ADDR_ORDER,1) PERM_ADDR_ORDER, 
       nvl(MAIL_ADDR_ORDER,1) MAIL_ADDR_ORDER, nvl(RESH_ADDR_ORDER,1) RESH_ADDR_ORDER, 
       nvl(RESH_UMBOS_ORDER,1) RESH_UMBOS_ORDER, 
       max(case when RESH_UMBOS_ORDER = 1 and ADDRESS_TYPE = 'RESH' and EFF_STATUS = 'A' then 'Y' else 'N' end) over (partition by P.PERSON_ID, P.SRC_SYS_ID) RESH_UMBOS_FLG,
       nvl(RESH_UMDAR_ORDER,1) RESH_UMDAR_ORDER, 
       max(case when RESH_UMDAR_ORDER = 1 and ADDRESS_TYPE = 'RESH' and EFF_STATUS = 'A' then 'Y' else 'N' end) over (partition by P.PERSON_ID, P.SRC_SYS_ID) RESH_UMDAR_FLG,
       nvl(RESH_UMLOW_ORDER,1) RESH_UMLOW_ORDER, 
       max(case when RESH_UMLOW_ORDER = 1 and ADDRESS_TYPE = 'RESH' and EFF_STATUS = 'A' then 'Y' else 'N' end) over (partition by P.PERSON_ID, P.SRC_SYS_ID) RESH_UMLOW_FLG,
       nvl(PML_HIST_ORDER,1) PML_HIST_ORDER, nvl(ADDR_ORDER,1) ADDR_ORDER, nvl(LASTUPDDTTM,to_date('01-JAN-1900')) LASTUPDDTTM, nvl(LASTUPDOPRID,'-') LASTUPDOPRID,
       'S' DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       SYSDATE CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       SYSDATE LASTUPD_EW_DTTM                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
  from PS_D_PERSON P
  left outer join Q2
    on P.PERSON_ID = Q2.PERSON_ID
   and P.SRC_SYS_ID = Q2.SRC_SYS_ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_ADDR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ADDR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_ADDR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_ADDR enable constraint PK_UM_D_PERSON_ADDR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_ADDR');

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

END UM_D_PERSON_ADDR_P;
/

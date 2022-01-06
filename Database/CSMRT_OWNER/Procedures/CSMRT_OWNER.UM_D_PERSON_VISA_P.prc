CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_VISA_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Old Tables               -- UM_D_PERSON_VISA / UM_D_PERSON_CS_VISA_VW
-- Loads target table       -- UM_D_PERSON_VISA
-- UM_D_PERSON_VISA         -- Dependent on PS_D_PERSON 
-- V01 4/10/2018            -- srikanth, pabbu converted to proc from sql
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_VISA';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_VISA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_VISA');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_VISA disable constraint PK_UM_D_PERSON_VISA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_VISA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_VISA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_VISA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_VISA';	
insert /*+ append */ into CSMRT_OWNER.UM_D_PERSON_VISA			
with X as ( 
select /*+ inline */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ inline parallel(8) */ 
       EMPLID PERSON_ID, DEPENDENT_ID, COUNTRY, VISA_PERMIT_TYPE, SRC_SYS_ID, EFFDT, 
       VISA_WRKPMT_NBR, VISA_WRKPMT_STATUS, STATUS_DT, DT_ISSUED, PLACE_ISSUED, 
       DURATION_TIME, DURATION_TYPE, ENTRY_DT, EXPIRATN_DT, ISSUING_AUTHORITY,
       DATA_ORIGIN, 
       row_number() over (partition by EMPLID, VISA_PERMIT_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_VISA_PMT_DATA
 where DATA_ORIGIN <> 'D'
   and EFFDT <= trunc(SYSDATE)),
       Q2 as ( 
select /*+ inline parallel(8) */ 
       Q1.PERSON_ID, Q1.DEPENDENT_ID, Q1.COUNTRY, Q1.VISA_PERMIT_TYPE, Q1.SRC_SYS_ID, Q1.EFFDT, 
       nvl(V.VISA_PERMIT_SID, 2147483646) VISA_PERMIT_SID, 
       Q1.VISA_WRKPMT_NBR, Q1.VISA_WRKPMT_STATUS, nvl(X1.XLATSHORTNAME,'-') VISA_WRKPMT_STATUS_SD, nvl(X1.XLATLONGNAME,'-') VISA_WRKPMT_STATUS_LD, 
       Q1.STATUS_DT, Q1.DT_ISSUED, Q1.PLACE_ISSUED, 
       Q1.DURATION_TIME, Q1.DURATION_TYPE, nvl(X2.XLATSHORTNAME,'-') DURATION_TYPE_SD, nvl(X2.XLATLONGNAME,'-') DURATION_TYPE_LD,  
       Q1.ENTRY_DT, Q1.EXPIRATN_DT, Q1.ISSUING_AUTHORITY, 
       nvl(V.DESCR, '-') VISA_PERMIT_TYPE_LD, 
       Q1.DATA_ORIGIN 
  from Q1
  left outer join UM_D_VISA_PERMIT V
    on Q1.VISA_PERMIT_TYPE = V.VISA_PERMIT_TYPE
   and Q1.COUNTRY = V.COUNTRY
   and Q1.SRC_SYS_ID = V.SRC_SYS_ID
  left outer join X X1
    on Q1.VISA_WRKPMT_STATUS = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'VISA_WRKPMT_STATUS'
   and X1.X_ORDER = 1  
  left outer join X X2
    on Q1.DURATION_TYPE = X2.FIELDVALUE
   and Q1.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'DURATION_TYPE'
   and X2.X_ORDER = 1  
 where Q1.Q_ORDER = 1),
       S as (
select /*+ inline parallel(8) */ 
       P.PERSON_ID, nvl(Q2.DEPENDENT_ID,'-') DEPENDENT_ID, nvl(Q2.COUNTRY,'-') COUNTRY, nvl(Q2.VISA_PERMIT_TYPE,'-') VISA_PERMIT_TYPE, P.SRC_SYS_ID,
       Q2.EFFDT, 
       P.PERSON_SID, 
       nvl(Q2.VISA_PERMIT_SID, 2147483646) VISA_PERMIT_SID,
       nvl(Q2.DURATION_TIME,0) DURATION_TIME, nvl(Q2.DURATION_TYPE,'-') DURATION_TYPE, 
       nvl(Q2.DURATION_TYPE_SD,'-') DURATION_TYPE_SD, nvl(Q2.DURATION_TYPE_LD,'-') DURATION_TYPE_LD,  
       Q2.ENTRY_DT, Q2.EXPIRATN_DT, 
       Q2.DT_ISSUED, nvl(Q2.PLACE_ISSUED,'-') PLACE_ISSUED, nvl(Q2.ISSUING_AUTHORITY,'-') ISSUING_AUTHORITY, 
       nvl(Q2.VISA_PERMIT_TYPE_LD,'-') VISA_PERMIT_TYPE_LD,
       nvl(Q2.VISA_WRKPMT_NBR,'-') VISA_WRKPMT_NBR, nvl(Q2.VISA_WRKPMT_STATUS,'-') VISA_WRKPMT_STATUS, 
       nvl(Q2.VISA_WRKPMT_STATUS_SD,'-') VISA_WRKPMT_STATUS_SD, nvl(Q2.VISA_WRKPMT_STATUS_LD,'-') VISA_WRKPMT_STATUS_LD, 
       Q2.STATUS_DT, 
       row_number() over (partition by P.PERSON_SID 
                              order by decode(Q2.DATA_ORIGIN, 'D', 9, 1),
                                       Q2.STATUS_DT desc,
                                       Q2.EFFDT desc,
                                       Q2.VISA_PERMIT_TYPE) VISA_ORDER, 
       least(P.DATA_ORIGIN,nvl(Q2.DATA_ORIGIN,'Z')) DATA_ORIGIN 
  from PS_D_PERSON P
  left outer join Q2
    on P.PERSON_ID = Q2.PERSON_ID
   and P.SRC_SYS_ID = Q2.SRC_SYS_ID)
select /*+ parallel(8) */
       S.PERSON_ID,
       S.DEPENDENT_ID,
       S.COUNTRY,
       S.VISA_PERMIT_TYPE,
       S.SRC_SYS_ID,
       S.EFFDT,
       S.PERSON_SID,
       S.VISA_PERMIT_SID,
       S.DURATION_TIME,
       S.DURATION_TYPE,
       S.DURATION_TYPE_SD,
       S.DURATION_TYPE_LD,
       S.ENTRY_DT,
       S.EXPIRATN_DT,
       S.DT_ISSUED,
       S.PLACE_ISSUED,
       S.ISSUING_AUTHORITY,
       S.VISA_PERMIT_TYPE_LD,
       S.VISA_WRKPMT_NBR,
       S.VISA_WRKPMT_STATUS,
       S.VISA_WRKPMT_STATUS_SD,
       S.VISA_WRKPMT_STATUS_LD,
       S.STATUS_DT,
       S.VISA_ORDER,
       S.DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_VISA rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_VISA',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_VISA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_VISA enable constraint PK_UM_D_PERSON_VISA';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_VISA');

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

END UM_D_PERSON_VISA_P;
/

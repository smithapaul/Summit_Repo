CREATE OR REPLACE PROCEDURE             "UM_D_CLASS_NOTES_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_D_CLASS_NOTES
--V01 12/11/2018             -- srikanth ,pabbu converted to proc from sql scripts
--V02 20/26/2019             -- Doucette ,James converted destructive load from Merge.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_NOTES';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CLASS_NOTES');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_NOTES disable constraint PK_UM_D_CLASS_NOTES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CLASS_NOTES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CLASS_NOTES';				
insert /*+ append */ into UM_D_CLASS_NOTES 
  with Q1 as (  
select CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, CLASS_SECTION CLASS_SECTION_CD, CLASS_NOTES_SEQ, SRC_SYS_ID, 
       PRINT_AT, CLASS_NOTE_NBR, PRINT_NOTE_W_O_CLS, DESCRLONG,  
       DATA_ORIGIN
  from CSSTG_OWNER.PS_CLASS_NOTES
 where DATA_ORIGIN <> 'D'),
       Q2 as ( 
select INSTITUTION, CLASS_NOTE_NBR, SRC_SYS_ID, 
       EFFDT, EFF_STATUS, DESCR, DESCRLONG,
       row_number() over (partition by INSTITUTION, CLASS_NOTE_NBR, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1900') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CLASS_NOTES_TBL), 
       S as (
select C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, 
       nvl(Q1.CLASS_NOTES_SEQ,0) CLASS_NOTES_SEQ, C.SRC_SYS_ID, 
       C.INSTITUTION_CD, C.CLASS_SID, 
       nvl(Q1.PRINT_AT,'-') PRINT_AT, nvl(Q1.CLASS_NOTE_NBR,'-') CLASS_NOTE_NBR, nvl(Q1.PRINT_NOTE_W_O_CLS,'-') PRINT_NOTE_W_O_CLS, 
       decode(nvl(Q1.CLASS_NOTE_NBR,'-'),'-',Q1.DESCRLONG,Q2.DESCRLONG) DESCRLONG, 
       least(C.DATA_ORIGIN,nvl(Q1.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CLASS C
  left outer join Q1 
    on C.CRSE_CD = Q1.CRSE_CD
   and C.CRSE_OFFER_NUM = Q1.CRSE_OFFER_NUM
   and C.TERM_CD = Q1.TERM_CD
   and C.SESSION_CD = Q1.SESSION_CD
   and C.CLASS_SECTION_CD = Q1.CLASS_SECTION_CD
   and C.SRC_SYS_ID = Q1.SRC_SYS_ID
  left outer join Q2
    on C.INSTITUTION_CD = Q2.INSTITUTION 
   and Q1.CLASS_NOTE_NBR = Q2.CLASS_NOTE_NBR 
   and C.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
select CRSE_CD, 
       CRSE_OFFER_NUM, 
       TERM_CD, 
       SESSION_CD, 
       CLASS_SECTION_CD, 
       CLASS_NOTES_SEQ, 
       SRC_SYS_ID,
       INSTITUTION_CD,
       CLASS_SID,	   
       PRINT_AT, 
       CLASS_NOTE_NBR, 
       PRINT_NOTE_W_O_CLS,
       DESCRLONG,	   
       DATA_ORIGIN,     -- Sept 2019  
       SYSDATE CREATED_EW_DTTM, 
       SYSDATE LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_NOTES rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_NOTES',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CLASS_NOTES';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_NOTES enable constraint PK_UM_D_CLASS_NOTES';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CLASS_NOTES');

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

END UM_D_CLASS_NOTES_P;
/

DROP PROCEDURE CSMRT_OWNER.UM_D_CLASS_MTG_PAT_P
/

--
-- UM_D_CLASS_MTG_PAT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_CLASS_MTG_PAT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads mart table UM_D_CLASS_MTG_PAT from PeopleSoft table PS_CLASS_MTG_PAT.
--
 --V01  SMT-xxxx 02/15/2018,    James Doucette
--                              Converted from SQL
 --V01  SMT-xxxx 01/16/2019,    Srikanth,Pabbu (chnaged from merge to turnc and load)
--                              
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CLASS_MTG_PAT';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CLASS_MTG_PAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CLASS_MTG_PAT');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_MTG_PAT disable constraint PK_UM_D_CLASS_MTG_PAT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CLASS_MTG_PAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CLASS_MTG_PAT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CLASS_MTG_PAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CLASS_MTG_PAT';				

insert /*+ append */ into CSMRT_OWNER.UM_D_CLASS_MTG_PAT
  with Q1 as (  
select /*+ parallel(8) inline */ 
       CRSE_ID CRSE_CD, CRSE_OFFER_NBR CRSE_OFFER_NUM, STRM TERM_CD, SESSION_CODE SESSION_CD, CLASS_SECTION CLASS_SECTION_CD, CLASS_MTG_NBR CLASS_MTG_NUM, SRC_SYS_ID, 
       FACILITY_ID, MEETING_TIME_START, MEETING_TIME_END, MON, TUES, WED, THURS, FRI, SAT, SUN, START_DT, END_DT, CRS_TOPIC_ID MTG_PAT_CRSE_TOPIC_ID, DESCR, STND_MTG_PAT, PRINT_TOPIC_ON_XCR,  
       DATA_ORIGIN
  from CSSTG_OWNER.PS_CLASS_MTG_PAT
where DATA_ORIGIN <> 'D'),    
       S as (
select /*+ parallel(8) inline */ 
       C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, 
       nvl(Q1.CLASS_MTG_NUM,0) CLASS_MTG_NUM, C.SRC_SYS_ID, 
       row_number() over (partition by C.CRSE_CD, C.CRSE_OFFER_NUM, C.TERM_CD, C.SESSION_CD, C.CLASS_SECTION_CD, C.SRC_SYS_ID
                              order by nvl(Q1.DATA_ORIGIN,'-') desc, decode(nvl(Q1.CLASS_MTG_NUM,0),0,999,Q1.CLASS_MTG_NUM)) CLASS_MTG_PAT_ORDER,  
       C.INSTITUTION_CD, C.CLASS_SID,
       nvl(F.FCLTY_SID, 2147483646) FCLTY_SID, 
--       Q1.MEETING_TIME_START, 
--       Q1.MEETING_TIME_END, 
       case when to_char(Q1.MEETING_TIME_START, 'HH24:MI') = '00:00' and to_char(Q1.MEETING_TIME_END, 'HH24:MI') = '00:00'  -- Oct 2019 
            then to_date(NULL)
            else to_date(to_char(Q1.MEETING_TIME_START, 'YYYY-MM-DD HH:MI AM'),'YYYY-MM-DD HH:MI AM')
        end MEETING_TIME_START, 
       case when to_char(Q1.MEETING_TIME_START, 'HH24:MI') = '00:00' and to_char(Q1.MEETING_TIME_END, 'HH24:MI') = '00:00'  -- Oct 2019 
            then to_date(NULL)
            else to_date(to_char(Q1.MEETING_TIME_END, 'YYYY-MM-DD HH:MI AM'),'YYYY-MM-DD HH:MI AM')
        end MEETING_TIME_END, 
       Q1.MON, Q1.TUES, Q1.WED, Q1.THURS, Q1.FRI, Q1.SAT, Q1.SUN, 
       (decode(Q1.MON,   'Y', 'M',  '') ||
        decode(Q1.TUES,  'Y', 'Tu', '') ||
        decode(Q1.WED,   'Y', 'W',  '') ||
        decode(Q1.THURS, 'Y', 'Th', '') ||
        decode(Q1.FRI,   'Y', 'F',  '') ||
        decode(Q1.SAT,   'Y', 'Sa', '') ||
        decode(Q1.SUN,   'Y', 'Su', '')) MTG_PAT_CD, 
       Q1.START_DT,  
       decode(to_char(Q1.MEETING_TIME_START, 'HH24:MI'),'00:00',to_char(NULL),to_char(Q1.MEETING_TIME_START, 'HH:MI AM')) START_TIME, 
       Q1.END_DT, 
       decode(to_char(Q1.MEETING_TIME_END, 'HH24:MI'),'00:00',to_char(NULL),to_char(Q1.MEETING_TIME_END, 'HH:MI AM')) END_TIME, 
       (case when to_char(Q1.MEETING_TIME_START, 'HH24:MI') = '00:00' and to_char(Q1.MEETING_TIME_END, 'HH24:MI') = '00:00'
             then to_char(NULL)
             when Q1.MEETING_TIME_START is NULL and Q1.MEETING_TIME_END is NULL
             then to_char(NULL)
             else to_char(Q1.MEETING_TIME_START, 'HH:MI AM') || ' - ' ||
                  to_char(Q1.MEETING_TIME_END, 'HH:MI AM')
          end) MEETING_TIME, 
       Q1.MTG_PAT_CRSE_TOPIC_ID, Q1.DESCR, Q1.STND_MTG_PAT, Q1.PRINT_TOPIC_ON_XCR,
       least(C.DATA_ORIGIN,nvl(Q1.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CLASS C 
  left outer join Q1   
    on C.CRSE_CD = Q1.CRSE_CD
   and C.CRSE_OFFER_NUM = Q1.CRSE_OFFER_NUM
   and C.TERM_CD = Q1.TERM_CD
  and C.SESSION_CD = Q1.SESSION_CD
   and C.CLASS_SECTION_CD = Q1.CLASS_SECTION_CD
   and C.SRC_SYS_ID = Q1.SRC_SYS_ID
  left outer join PS_D_FCLTY F  
    on C.INSTITUTION_CD = F.SETID
   and Q1.FACILITY_ID = F.FCLTY_ID
   and Q1.SRC_SYS_ID = F.SRC_SYS_ID
   and F.DATA_ORIGIN <> 'D') 
select /*+ parallel(8) */ ROWNUM CLASS_MTG_PAT_SID, 
       CRSE_CD, CRSE_OFFER_NUM, TERM_CD, SESSION_CD, CLASS_SECTION_CD, CLASS_MTG_NUM, SRC_SYS_ID, 
       CLASS_MTG_PAT_ORDER, INSTITUTION_CD, CLASS_SID, FCLTY_SID, MEETING_TIME_START, MEETING_TIME_END, 
       MON, TUES, WED, THURS, FRI, SAT, SUN, MTG_PAT_CD, START_DT, START_TIME, END_DT, END_TIME, MEETING_TIME, 
       MTG_PAT_CRSE_TOPIC_ID, DESCR, STND_MTG_PAT, PRINT_TOPIC_ON_XCR, 
       DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CLASS_MTG_PAT rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_CLASS_MTG_PAT',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CLASS_MTG_PAT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CLASS_MTG_PAT enable constraint PK_UM_D_CLASS_MTG_PAT'; 
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CLASS_MTG_PAT');

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

END UM_D_CLASS_MTG_PAT_P;
/

DROP PROCEDURE CSMRT_OWNER.AM_PS_TERM_TBL_P
/

--
-- AM_PS_TERM_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_TERM_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TERM_TBL from PeopleSoft table PS_TERM_TBL.
--
 --V01  SMT-xxxx 08/25/2017,    Preethi Lodha
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TERM_TBL';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_TERM_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TERM_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_TERM_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TERM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TERM_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TERM_TBL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(STRM),'-') STRM,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
to_date(to_char(case when TERM_BEGIN_DT < '01-JAN-1800' then NULL else TERM_BEGIN_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') TERM_BEGIN_DT,
to_date(to_char(case when TERM_END_DT < '01-JAN-1800' then NULL else TERM_END_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') TERM_END_DT,
nvl(trim(SESSION_CODE),'-') SESSION_CODE,
nvl(WEEKS_OF_INSTRUCT,0) WEEKS_OF_INSTRUCT,
nvl(trim(TERM_CATEGORY),'-') TERM_CATEGORY,
nvl(trim(ACAD_YEAR),'-') ACAD_YEAR,
nvl(trim(TRANSCIPT_DT_PRT),'-') TRANSCIPT_DT_PRT,
nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE,
to_date(to_char(case when SIXTY_PCT_DT < '01-JAN-1800' then NULL else SIXTY_PCT_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SIXTY_PCT_DT,
nvl(trim(USE_DYN_CLASS_DATE),'-') USE_DYN_CLASS_DATE,
'-' INCLUDE_IN_SS,
to_date(to_char(case when SSR_TRMAC_LAST_DT < '01-JAN-1800' then NULL else SSR_TRMAC_LAST_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_TRMAC_LAST_DT,
to_date(to_char('01/01/1900'), 'MM/DD/YYYY' ) SSR_PLNDISPONLY_DT,
to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )  SSR_SSENRLAVAIL_DT
from SYSADM.PS_TERM_TBL@AMSOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_TBL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.ACAD_CAREER = S.ACAD_CAREER and
T.STRM = S.STRM and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.TERM_BEGIN_DT = S.TERM_BEGIN_DT,
T.TERM_END_DT = S.TERM_END_DT,
T.SESSION_CODE = S.SESSION_CODE,
T.WEEKS_OF_INSTRUCT = S.WEEKS_OF_INSTRUCT,
T.TERM_CATEGORY = S.TERM_CATEGORY,
T.ACAD_YEAR = S.ACAD_YEAR,
T.TRANSCIPT_DT_PRT = S.TRANSCIPT_DT_PRT,
T.HOLIDAY_SCHEDULE = S.HOLIDAY_SCHEDULE,
T.SIXTY_PCT_DT = S.SIXTY_PCT_DT,
T.USE_DYN_CLASS_DATE = S.USE_DYN_CLASS_DATE,
--T.INCLUDE_IN_SS = S.INCLUDE_IN_SS,
T.SSR_TRMAC_LAST_DT = S.SSR_TRMAC_LAST_DT,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.TERM_BEGIN_DT <> S.TERM_BEGIN_DT or
T.TERM_END_DT <> S.TERM_END_DT or
T.SESSION_CODE <> S.SESSION_CODE or
T.WEEKS_OF_INSTRUCT <> S.WEEKS_OF_INSTRUCT or
T.TERM_CATEGORY <> S.TERM_CATEGORY or
T.ACAD_YEAR <> S.ACAD_YEAR or
T.TRANSCIPT_DT_PRT <> S.TRANSCIPT_DT_PRT or
T.HOLIDAY_SCHEDULE <> S.HOLIDAY_SCHEDULE or
T.SIXTY_PCT_DT <> S.SIXTY_PCT_DT or
T.USE_DYN_CLASS_DATE <> S.USE_DYN_CLASS_DATE or
--T.INCLUDE_IN_SS <> S.INCLUDE_IN_SS or
T.SSR_TRMAC_LAST_DT <> S.SSR_TRMAC_LAST_DT or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.ACAD_CAREER,
T.STRM,
T.SRC_SYS_ID,
T.DESCR,
T.DESCRSHORT,
T.TERM_BEGIN_DT,
T.TERM_END_DT,
T.SESSION_CODE,
T.WEEKS_OF_INSTRUCT,
T.TERM_CATEGORY,
T.ACAD_YEAR,
T.TRANSCIPT_DT_PRT,
T.HOLIDAY_SCHEDULE,
T.SIXTY_PCT_DT,
T.USE_DYN_CLASS_DATE,
T.INCLUDE_IN_SS,
T.SSR_TRMAC_LAST_DT,
T.SSR_PLNDISPONLY_DT,
T.SSR_SSENRLAVAIL_DT,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.INSTITUTION,
S.ACAD_CAREER,
S.STRM,
'CS90',
S.DESCR,
S.DESCRSHORT,
S.TERM_BEGIN_DT,
S.TERM_END_DT,
S.SESSION_CODE,
S.WEEKS_OF_INSTRUCT,
S.TERM_CATEGORY,
S.ACAD_YEAR,
S.TRANSCIPT_DT_PRT,
S.HOLIDAY_SCHEDULE,
S.SIXTY_PCT_DT,
S.USE_DYN_CLASS_DATE,
'-',
S.SSR_TRMAC_LAST_DT,
to_date(to_char('01/01/1900'), 'MM/DD/YYYY' ) ,
to_date(to_char('01/01/1900'), 'MM/DD/YYYY' ) , 
'N',
'S',
sysdate,
sysdate,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TERM_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TERM_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TERM_TBL';
update AMSTG_OWNER.PS_TERM_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, STRM
   from AMSTG_OWNER.PS_TERM_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, STRM
   from SYSADM.PS_TERM_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
      and  T.ACAD_CAREER = S.ACAD_CAREER
      and T.STRM = S.STRM
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TERM_TBL'
;

strSqlCommand := 'commit';
commit;


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

END AM_PS_TERM_TBL_P;
/

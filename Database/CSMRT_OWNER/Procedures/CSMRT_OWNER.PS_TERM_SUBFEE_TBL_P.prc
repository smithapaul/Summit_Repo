DROP PROCEDURE CSMRT_OWNER.PS_TERM_SUBFEE_TBL_P
/

--
-- PS_TERM_SUBFEE_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_TERM_SUBFEE_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_TERM_SUBFEE_TBL';

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_TERM_SUBFEE_TBL', 'Y');

COMMIT; 

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_TERM_SUBFEE_TBL';
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TERM_SUBFEE_TBL from PeopleSoft table PS_TERM_SUBFEE_TBL.
--
-- V01  SMT-7550 11/29/2017,    Jim Doucette
--                              New Table
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_TERM_SUBFEE_TBL';
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

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_TERM_SUBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TERM_SUBFEE_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_TERM_SUBFEE_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_TERM_SUBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_TERM_SUBFEE_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_TERM_SUBFEE_TBL T 
using (select /*+ full(S) */ 
	nvl(trim(SETID),'-') SETID,
	nvl(trim(FEE_CODE),'-') FEE_CODE,
	nvl(trim(STRM),'-') STRM,
	nvl(trim(SESSION_CODE),'-') SESSION_CODE,
	nvl(trim(SUB_FEE_CODE),'-') SUB_FEE_CODE,
	nvl(trim(INSTITUTION),'-') INSTITUTION,
	nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
	nvl(trim(ACAD_GROUP),'-') ACAD_GROUP,
	nvl(trim(SUBJECT),'-') SUBJECT,
	nvl(trim(CAMPUS),'-') CAMPUS,
	nvl(trim(LOCATION),'-') LOCATION,
	nvl(trim(INSTRUCTION_MODE),'-') INSTRUCTION_MODE,
	nvl(trim(ACAD_PROG),'-') ACAD_PROG,
	nvl(trim(SSR_COHORT_YR),'-') SSR_COHORT_YR,
	nvl(UNIT_FROM,0) UNIT_FROM,
	nvl(UNIT_TO,0) UNIT_TO,
	nvl(trim(EQUATION_NAME),'-') EQUATION_NAME,
	nvl(AMT_PER_UNIT,0) AMT_PER_UNIT,
	nvl(FLAT_AMT,0) FLAT_AMT
from SYSADM.PS_TERM_SUBFEE_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_SUBFEE_TBL') ) S 
 on (
	T.SETID = S.SETID and
	T.FEE_CODE = S.FEE_CODE and
	T.STRM = S.STRM and
	T.SESSION_CODE = S.SESSION_CODE and
	T.SUB_FEE_CODE = S.SUB_FEE_CODE and
	T.INSTITUTION = S.INSTITUTION and
	T.ACAD_CAREER = S.ACAD_CAREER and
	T.ACAD_GROUP = S.ACAD_GROUP and
	T.SUBJECT = S.SUBJECT and
	T.CAMPUS = S.CAMPUS and
	T.LOCATION = S.LOCATION and
	T.INSTRUCTION_MODE = S.INSTRUCTION_MODE and
	T.ACAD_PROG = S.ACAD_PROG and
	T.SSR_COHORT_YR = S.SSR_COHORT_YR and
	T.UNIT_FROM = S.UNIT_FROM and
	T.UNIT_TO = S.UNIT_TO and
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.EQUATION_NAME = S.EQUATION_NAME, 
	T.AMT_PER_UNIT = S.AMT_PER_UNIT, 
	T.FLAT_AMT = S.FLAT_AMT, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where
	T.EQUATION_NAME <> S.EQUATION_NAME or
	T.AMT_PER_UNIT <> S.AMT_PER_UNIT or
	T.FLAT_AMT <> S.FLAT_AMT or
	T.DATA_ORIGIN = 'D'
when not matched then
insert ( 
	T.SETID, 
	T.FEE_CODE,
	T.STRM,
	T.SESSION_CODE,
	T.SUB_FEE_CODE,
	T.INSTITUTION, 
	T.ACAD_CAREER, 
	T.ACAD_GROUP,
	T.SUBJECT, 
	T.CAMPUS,
	T.LOCATION,
	T.INSTRUCTION_MODE,
	T.ACAD_PROG, 
	T.SSR_COHORT_YR, 
	T.UNIT_FROM, 
	T.UNIT_TO, 
	T.SRC_SYS_ID,
	T.EQUATION_NAME, 
	T.AMT_PER_UNIT,
	T.FLAT_AMT,
	T.LOAD_ERROR,
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	)
values ( 
	S.SETID, 
	S.FEE_CODE,
	S.STRM,
	S.SESSION_CODE,
	S.SUB_FEE_CODE,
	S.INSTITUTION, 
	S.ACAD_CAREER, 
	S.ACAD_GROUP,
	S.SUBJECT, 
	S.CAMPUS,
	S.LOCATION,
	S.INSTRUCTION_MODE,
	S.ACAD_PROG, 
	S.SSR_COHORT_YR, 
	S.UNIT_FROM, 
	S.UNIT_TO, 
	'CS90',
	S.EQUATION_NAME, 
	S.AMT_PER_UNIT,
	S.FLAT_AMT,
	'N', 
	'S', 
	sysdate, 
	sysdate, 
	1234)
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_SUBFEE_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_SUBFEE_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TERM_SUBFEE_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_TERM_SUBFEE_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_TERM_SUBFEE_TBL';
update CSSTG_OWNER.PS_TERM_SUBFEE_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, 
        FEE_CODE, 
        STRM, 
        SESSION_CODE, 
        SUB_FEE_CODE, 
        INSTITUTION, 
        ACAD_CAREER, 
        ACAD_GROUP, 
        SUBJECT, 
        CAMPUS, 
        LOCATION, 
        INSTRUCTION_MODE, 
        ACAD_PROG, 
        SSR_COHORT_YR, 
        UNIT_FROM, 
        UNIT_TO
   from CSSTG_OWNER.PS_TERM_SUBFEE_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_SUBFEE_TBL') = 'Y'
  minus
 select nvl(trim(SETID),'-') SETID,
	nvl(trim(FEE_CODE),'-') FEE_CODE,
	nvl(trim(STRM),'-') STRM,
	nvl(trim(SESSION_CODE),'-') SESSION_CODE,
	nvl(trim(SUB_FEE_CODE),'-') SUB_FEE_CODE,
	nvl(trim(INSTITUTION),'-') INSTITUTION,
	nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
	nvl(trim(ACAD_GROUP),'-') ACAD_GROUP,
	nvl(trim(SUBJECT),'-') SUBJECT,
	nvl(trim(CAMPUS),'-') CAMPUS,
	nvl(trim(LOCATION),'-') LOCATION,
	nvl(trim(INSTRUCTION_MODE),'-') INSTRUCTION_MODE,
	nvl(trim(ACAD_PROG),'-') ACAD_PROG,
	nvl(trim(SSR_COHORT_YR),'-') SSR_COHORT_YR,
	nvl(UNIT_FROM,0) UNIT_FROM,
	nvl(UNIT_TO,0) UNIT_TO
   from SYSADM.PS_TERM_SUBFEE_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TERM_SUBFEE_TBL') = 'Y'
   ) S
 where  T.SETID = S.SETID
   and  T.FEE_CODE = S.FEE_CODE
   and  T.STRM = S.STRM
   and  T.SESSION_CODE = S.SESSION_CODE
   and  T.SUB_FEE_CODE = S.SUB_FEE_CODE
   and  T.INSTITUTION = S.INSTITUTION
   and  T.ACAD_CAREER = S.ACAD_CAREER
   and  T.ACAD_GROUP = S.ACAD_GROUP
   and  T.SUBJECT = S.SUBJECT
   and  T.CAMPUS = S.CAMPUS
   and  T.LOCATION = S.LOCATION
   and  T.INSTRUCTION_MODE = S.INSTRUCTION_MODE
   and  T.ACAD_PROG = S.ACAD_PROG
   and  T.SSR_COHORT_YR = S.SSR_COHORT_YR
   and  T.UNIT_FROM =  S.UNIT_FROM
   and  T.UNIT_TO = S.UNIT_TO
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TERM_SUBFEE_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TERM_SUBFEE_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TERM_SUBFEE_TBL'
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

END PS_TERM_SUBFEE_TBL_P;
/

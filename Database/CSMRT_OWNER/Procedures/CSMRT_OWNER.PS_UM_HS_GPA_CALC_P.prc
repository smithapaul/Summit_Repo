DROP PROCEDURE CSMRT_OWNER.PS_UM_HS_GPA_CALC_P
/

--
-- PS_UM_HS_GPA_CALC_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_HS_GPA_CALC_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_HS_GPA_CALC'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_UM_HS_GPA_CALC', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_HS_GPA_CALC'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_HS_GPA_CALC from PeopleSoft table PS_UM_HS_GPA_CALC.
--
-- V01  SMT-xxxx 05/16/2017,    Jim Doucette
--                              Converted from PS_UM_HS_GPA_CALC.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_HS_GPA_CALC';
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
 where TABLE_NAME = 'PS_UM_HS_GPA_CALC'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_HS_GPA_CALC@SASOURCE S)
 where TABLE_NAME = 'PS_UM_HS_GPA_CALC'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_HS_GPA_CALC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_HS_GPA_CALC';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_HS_GPA_CALC T 
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(EXT_ORG_ID),'-') EXT_ORG_ID, 
    nvl(trim(EXT_CAREER),'-') EXT_CAREER, 
    nvl(EXT_DATA_NBR,0) EXT_DATA_NBR, 
    nvl(trim(EXT_SUMM_TYPE),'-') EXT_SUMM_TYPE, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(EXT_COURSE_NBR,0) EXT_COURSE_NBR, 
    nvl(trim(SCHOOL_SUBJECT),'-') SCHOOL_SUBJECT, 
    nvl(trim(SCHOOL_CRSE_NBR),'-') SCHOOL_CRSE_NBR, 
    nvl(trim(GPA_TYPE),'-') GPA_TYPE, 
    nvl(EXT_GPA,0) EXT_GPA, 
    nvl(EXT_UNITS,0) EXT_UNITS, 
    nvl(trim(UM_HS_GPA_WT_TYPE),'-') UM_HS_GPA_WT_TYPE, 
    nvl(UM_HS_GPA_WEIGHT,0) UM_HS_GPA_WEIGHT, 
    nvl(CONVERT_GPA,0) CONVERT_GPA
  from SYSADM.PS_UM_HS_GPA_CALC@SASOURCE S
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_HS_GPA_CALC')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8 ) S 
 on ( 
    T.EMPLID = S.EMPLID and 
    T.EXT_ORG_ID = S.EXT_ORG_ID and 
    T.EXT_CAREER = S.EXT_CAREER and 
    T.EXT_DATA_NBR = S.EXT_DATA_NBR and 
    T.EXT_SUMM_TYPE = S.EXT_SUMM_TYPE and 
    T.INSTITUTION = S.INSTITUTION and 
    T.EXT_COURSE_NBR = S.EXT_COURSE_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SCHOOL_SUBJECT = S.SCHOOL_SUBJECT,
    T.SCHOOL_CRSE_NBR = S.SCHOOL_CRSE_NBR,
    T.GPA_TYPE = S.GPA_TYPE,
    T.EXT_GPA = S.EXT_GPA,
    T.EXT_UNITS = S.EXT_UNITS,
    T.UM_HS_GPA_WT_TYPE = S.UM_HS_GPA_WT_TYPE,
    T.UM_HS_GPA_WEIGHT = S.UM_HS_GPA_WEIGHT,
    T.CONVERT_GPA = S.CONVERT_GPA,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SCHOOL_SUBJECT <> S.SCHOOL_SUBJECT or 
    T.SCHOOL_CRSE_NBR <> S.SCHOOL_CRSE_NBR or 
    T.GPA_TYPE <> S.GPA_TYPE or 
    T.EXT_GPA <> S.EXT_GPA or 
    T.EXT_UNITS <> S.EXT_UNITS or 
    T.UM_HS_GPA_WT_TYPE <> S.UM_HS_GPA_WT_TYPE or 
    T.UM_HS_GPA_WEIGHT <> S.UM_HS_GPA_WEIGHT or 
    T.CONVERT_GPA <> S.CONVERT_GPA or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.EXT_ORG_ID, 
    T.EXT_CAREER, 
    T.EXT_DATA_NBR, 
    T.EXT_SUMM_TYPE,
    T.INSTITUTION,
    T.EXT_COURSE_NBR, 
    T.SRC_SYS_ID, 
    T.SCHOOL_SUBJECT, 
    T.SCHOOL_CRSE_NBR,
    T.GPA_TYPE, 
    T.EXT_GPA,
    T.EXT_UNITS,
    T.UM_HS_GPA_WT_TYPE,
    T.UM_HS_GPA_WEIGHT, 
    T.CONVERT_GPA,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
) 
values (
    S.EMPLID, 
    S.EXT_ORG_ID, 
    S.EXT_CAREER, 
    S.EXT_DATA_NBR, 
    S.EXT_SUMM_TYPE,
    S.INSTITUTION,
    S.EXT_COURSE_NBR, 
    'CS90', 
    S.SCHOOL_SUBJECT, 
    S.SCHOOL_CRSE_NBR,
    S.GPA_TYPE, 
    S.EXT_GPA,
    S.EXT_UNITS,
    S.UM_HS_GPA_WT_TYPE,
    S.UM_HS_GPA_WEIGHT, 
    S.CONVERT_GPA,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_HS_GPA_CALC rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_HS_GPA_CALC',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_HS_GPA_CALC';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_HS_GPA_CALC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_HS_GPA_CALC';
update CSSTG_OWNER.PS_UM_HS_GPA_CALC T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUMM_TYPE, INSTITUTION, EXT_COURSE_NBR
   from CSSTG_OWNER.PS_UM_HS_GPA_CALC T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_HS_GPA_CALC') = 'Y'
  minus
 select EMPLID, EXT_ORG_ID, EXT_CAREER, EXT_DATA_NBR, EXT_SUMM_TYPE, INSTITUTION, EXT_COURSE_NBR
   from SYSADM.PS_UM_HS_GPA_CALC@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_HS_GPA_CALC') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.EXT_ORG_ID = S.EXT_ORG_ID
   and T.EXT_CAREER = S.EXT_CAREER
   and T.EXT_DATA_NBR = S.EXT_DATA_NBR
   and T.EXT_SUMM_TYPE = S.EXT_SUMM_TYPE
   and T.INSTITUTION = S.INSTITUTION
   and T.EXT_COURSE_NBR = S.EXT_COURSE_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_HS_GPA_CALC rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_HS_GPA_CALC',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_HS_GPA_CALC'
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

END PS_UM_HS_GPA_CALC_P;
/

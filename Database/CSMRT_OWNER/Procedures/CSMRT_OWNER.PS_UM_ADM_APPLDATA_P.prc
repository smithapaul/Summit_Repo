DROP PROCEDURE CSMRT_OWNER.PS_UM_ADM_APPLDATA_P
/

--
-- PS_UM_ADM_APPLDATA_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_UM_ADM_APPLDATA_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_ADM_APPLDATA'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_UM_ADM_APPLDATA', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_UM_ADM_APPLDATA'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_ADM_APPLDATA from PeopleSoft table PS_UM_ADM_APPLDATA.
--
-- V01  SMT-xxxx 05/30/2017,    Jim Doucette
--                              Converted from PS_UM_ADM_APPLDATA.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_ADM_APPLDATA';
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
 where TABLE_NAME = 'PS_UM_ADM_APPLDATA'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_ADM_APPLDATA@SASOURCE S)
 where TABLE_NAME = 'PS_UM_ADM_APPLDATA'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_ADM_APPLDATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_ADM_APPLDATA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_ADM_APPLDATA T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
    nvl(trim(ADM_APPL_NBR),'-') ADM_APPL_NBR, 
    nvl(trim(UM_ACAD_PROG1),'-')  UM_ACAD_PROG1,
    nvl(trim(UM_ACAD_PLAN1),'-')  UM_ACAD_PLAN1,
    nvl(trim(UM_ACAD_SUB_PLAN1),'-')  UM_ACAD_SUB_PLAN1,
    nvl(trim(UM_MANUAL_COMPLETE),'-')  UM_MANUAL_COMPLETE,
    to_date(to_char(case when UM_COMPLETED_DT < '01-JAN-1800' then NULL 
                    else UM_COMPLETED_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') UM_COMPLETED_DT, 
    nvl(trim(UM_CA_TESTING_PLAN),'-')  UM_CA_TESTING_PLAN,
    nvl(trim(UM_CA_FIRST_GEN),'-') UM_CA_FIRST_GEN
  from SYSADM.PS_UM_ADM_APPLDATA@SASOURCE S 
 where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APPLDATA')
   and EMPLID BETWEEN '00000000' AND '99999999'
   and length(EMPLID) = 8 ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
    T.ADM_APPL_NBR = S.ADM_APPL_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.UM_ACAD_PROG1 = S.UM_ACAD_PROG1,
    T.UM_ACAD_PLAN1 = S.UM_ACAD_PLAN1,
    T.UM_ACAD_SUB_PLAN1 = S.UM_ACAD_SUB_PLAN1,
    T.UM_MANUAL_COMPLETE = S.UM_MANUAL_COMPLETE,
    T.UM_COMPLETED_DT = S.UM_COMPLETED_DT,
    T.UM_CA_TESTING_PLAN = S.UM_CA_TESTING_PLAN,
    T.UM_CA_FIRST_GEN = S.UM_CA_FIRST_GEN,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.UM_ACAD_PROG1),0) <> nvl(trim(S.UM_ACAD_PROG1),0) or 
    nvl(trim(T.UM_ACAD_PLAN1),0) <> nvl(trim(S.UM_ACAD_PLAN1),0) or 
    nvl(trim(T.UM_ACAD_SUB_PLAN1),0) <> nvl(trim(S.UM_ACAD_SUB_PLAN1),0) or 
    nvl(trim(T.UM_MANUAL_COMPLETE),0) <> nvl(trim(S.UM_MANUAL_COMPLETE),0) or 
    nvl(trim(T.UM_COMPLETED_DT),0) <> nvl(trim(S.UM_COMPLETED_DT),0) or 
    nvl(trim(T.UM_CA_TESTING_PLAN),0) <> nvl(trim(S.UM_CA_TESTING_PLAN),0) or 
    nvl(trim(T.UM_CA_FIRST_GEN),0) <> nvl(trim(S.UM_CA_FIRST_GEN),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.ACAD_CAREER,
    T.STDNT_CAR_NBR,
    T.ADM_APPL_NBR, 
    T.SRC_SYS_ID, 
    T.UM_ACAD_PROG1,
    T.UM_ACAD_PLAN1,
    T.UM_ACAD_SUB_PLAN1,
    T.UM_MANUAL_COMPLETE, 
    T.UM_COMPLETED_DT,
    T.UM_CA_TESTING_PLAN, 
    T.UM_CA_FIRST_GEN,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.ACAD_CAREER,
    S.STDNT_CAR_NBR,
    S.ADM_APPL_NBR, 
    'CS90', 
    S.UM_ACAD_PROG1,
    S.UM_ACAD_PLAN1,
    S.UM_ACAD_SUB_PLAN1,
    S.UM_MANUAL_COMPLETE, 
    S.UM_COMPLETED_DT,
    S.UM_CA_TESTING_PLAN, 
    S.UM_CA_FIRST_GEN,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_ADM_APPLDATA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_APPLDATA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_ADM_APPLDATA';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_APPLDATA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_ADM_APPLDATA';
update CSSTG_OWNER.PS_UM_ADM_APPLDATA T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR
   from CSSTG_OWNER.PS_UM_ADM_APPLDATA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APPLDATA') = 'Y'
  minus
 select EMPLID, ACAD_CAREER, STDNT_CAR_NBR, ADM_APPL_NBR
   from SYSADM.PS_UM_ADM_APPLDATA@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ADM_APPLDATA') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
   and T.ADM_APPL_NBR = S.ADM_APPL_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_ADM_APPLDATA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ADM_APPLDATA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_ADM_APPLDATA'
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

END PS_UM_ADM_APPLDATA_P;
/

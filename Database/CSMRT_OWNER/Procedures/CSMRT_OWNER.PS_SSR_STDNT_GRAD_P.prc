CREATE OR REPLACE PROCEDURE             "PS_SSR_STDNT_GRAD_P"
   AUTHID CURRENT_USER
IS
   /*
   -- Run before the first time
   DELETE
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD'

   INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
   (TABLE_NAME, DELETE_FLG)
   VALUES
   ('PS_SSR_STDNT_GRAD', 'Y')

   SELECT *
   FROM CSSTG_OWNER.UM_STAGE_JOBS
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD'
   */


   ------------------------------------------------------------------------
   -- George Adams
   --
   -- Loads stage table PS_SSR_STDNT_GRAD from PeopleSoft table PS_SSR_STDNT_GRAD.
   --
   -- V01  SMT-7992 09/13/2018,    Debra Martin
   --                              New stage table from PeopleSoft.
   --
   ------------------------------------------------------------------------

   strMartId          VARCHAR2 (50) := 'CSW';
   strProcessName     VARCHAR2 (100) := 'PS_SSR_STDNT_GRAD';
   intProcessSid      INTEGER;
   dtProcessStart     DATE := SYSDATE;
   strMessage01       VARCHAR2 (4000);
   strMessage02       VARCHAR2 (512);
   strMessage03       VARCHAR2 (512) := '';
   strNewLine         VARCHAR2 (2) := CHR (13) || CHR (10);
   strSqlCommand      VARCHAR2 (32767) := '';
   strSqlDynamic      VARCHAR2 (32767) := '';
   strClientInfo      VARCHAR2 (100);
   intRowCount        INTEGER;
   intTotalRowCount   INTEGER := 0;
   numSqlCode         NUMBER;
   strSqlErrm         VARCHAR2 (4000);
   intTries           INTEGER;
   
   
BEGIN
   strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
   DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT (
      i_MartId             => strMartId,
      i_ProcessName        => strProcessName,
      i_ProcessStartTime   => dtProcessStart,
      o_ProcessSid         => intProcessSid);

   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);


   strSqlCommand := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Reading', START_DT = SYSDATE, END_DT = NULL
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Merging',
          NEW_MAX_SCN =
             (SELECT /*+ full(S) */
                    MAX (ORA_ROWSCN)
                FROM SYSADM.PS_SSR_STDNT_GRAD@SASOURCE S)
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Merging data into CSSTG_OWNER.PS_SSR_STDNT_GRAD';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'merge into CSSTG_OWNER.PS_SSR_STDNT_GRAD';
   
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_SSR_STDNT_GRAD T 
using (select /*+ full(S) */ 
	nvl(trim(EMPLID),'-') EMPLID, 
	nvl(trim(INSTITUTION),'-') INSTITUTION, 
	nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
	nvl(STDNT_CAR_NBR,0) STDNT_CAR_NBR, 
	nvl(trim(ACAD_PROG),'-') ACAD_PROG, 
	nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, 
	nvl(trim(DEGREE),'-') DEGREE, 
	nvl(trim(SSR_GRAD_STATUS),'-') SSR_GRAD_STATUS, 
	to_date(to_char(case when STATUS_DT < '01-JAN-1800' then NULL else STATUS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') STATUS_DT, 
	nvl(trim(OPRID_LAST_UPDT),'-') OPRID_LAST_UPDT, 
	SSR_REVIEW_DATETM SSR_REVIEW_DATETM, 
	SSR_GRAD_REV_DTTM SSR_GRAD_REV_DTTM, 
	nvl(trim(SCC_ROW_ADD_OPRID),'-') SCC_ROW_ADD_OPRID, 
	SCC_ROW_ADD_DTTM SCC_ROW_ADD_DTTM, 
	nvl(trim(SCC_ROW_UPD_OPRID),'-') SCC_ROW_UPD_OPRID, 
	SCC_ROW_UPD_DTTM SCC_ROW_UPD_DTTM
from SYSADM.PS_SSR_STDNT_GRAD@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_SSR_STDNT_GRAD') ) S 
 on ( 
	T.EMPLID = S.EMPLID and 
	T.INSTITUTION = S.INSTITUTION and 
	T.ACAD_CAREER = S.ACAD_CAREER and 
	T.STDNT_CAR_NBR = S.STDNT_CAR_NBR and 
	T.ACAD_PROG = S.ACAD_PROG and 
	T.EXP_GRAD_TERM = S.EXP_GRAD_TERM and 
	T.DEGREE = S.DEGREE and 
	T.SRC_SYS_ID = 'CS90') 
when matched then update set 
	T.SSR_GRAD_STATUS = S.SSR_GRAD_STATUS, 
	T.STATUS_DT = S.STATUS_DT, 
	T.OPRID_LAST_UPDT = S.OPRID_LAST_UPDT, 
	T.SSR_REVIEW_DATETM = S.SSR_REVIEW_DATETM, 
	T.SSR_GRAD_REV_DTTM = S.SSR_GRAD_REV_DTTM, 
	T.SCC_ROW_ADD_OPRID = S.SCC_ROW_ADD_OPRID, 
	T.SCC_ROW_ADD_DTTM = S.SCC_ROW_ADD_DTTM, 
	T.SCC_ROW_UPD_OPRID = S.SCC_ROW_UPD_OPRID, 
	T.SCC_ROW_UPD_DTTM = S.SCC_ROW_UPD_DTTM, 
	T.DATA_ORIGIN = 'S', 
	T.LASTUPD_EW_DTTM = sysdate, 
	T.BATCH_SID = 1234 
where 
	T.SSR_GRAD_STATUS <> S.SSR_GRAD_STATUS or 
	nvl(trim(T.STATUS_DT),0) <> nvl(trim(S.STATUS_DT),0) or 
	T.OPRID_LAST_UPDT <> S.OPRID_LAST_UPDT or 
	nvl(trim(T.SSR_REVIEW_DATETM),0) <> nvl(trim(S.SSR_REVIEW_DATETM),0) or 
	nvl(trim(T.SSR_GRAD_REV_DTTM),0) <> nvl(trim(S.SSR_GRAD_REV_DTTM),0) or 
	T.SCC_ROW_ADD_OPRID <> S.SCC_ROW_ADD_OPRID or 
	nvl(trim(T.SCC_ROW_ADD_DTTM),0) <> nvl(trim(S.SCC_ROW_ADD_DTTM),0) or 
	T.SCC_ROW_UPD_OPRID <> S.SCC_ROW_UPD_OPRID or 
	nvl(trim(T.SCC_ROW_UPD_DTTM),0) <> nvl(trim(S.SCC_ROW_UPD_DTTM),0) or 
	T.DATA_ORIGIN = 'D' 
when not matched then 
insert ( 
	T.EMPLID, 
	T.INSTITUTION, 
	T.ACAD_CAREER, 
	T.STDNT_CAR_NBR, 
	T.ACAD_PROG, 
	T.EXP_GRAD_TERM, 
	T.DEGREE, 
	T.SRC_SYS_ID, 
	T.SSR_GRAD_STATUS, 
	T.STATUS_DT, 
	T.OPRID_LAST_UPDT, 
	T.SSR_REVIEW_DATETM, 
	T.SSR_GRAD_REV_DTTM, 
	T.SCC_ROW_ADD_OPRID, 
	T.SCC_ROW_ADD_DTTM, 
	T.SCC_ROW_UPD_OPRID, 
	T.SCC_ROW_UPD_DTTM, 
	T.LOAD_ERROR, 
	T.DATA_ORIGIN, 
	T.CREATED_EW_DTTM, 
	T.LASTUPD_EW_DTTM, 
	T.BATCH_SID
	) 
values ( 
	S.EMPLID, 
	S.INSTITUTION, 
	S.ACAD_CAREER, 
	S.STDNT_CAR_NBR, 
	S.ACAD_PROG, 
	S.EXP_GRAD_TERM, 
	S.DEGREE, 
	'CS90', 
	S.SSR_GRAD_STATUS, 
	S.STATUS_DT, 
	S.OPRID_LAST_UPDT, 
	S.SSR_REVIEW_DATETM, 
	S.SSR_GRAD_REV_DTTM, 
	S.SCC_ROW_ADD_OPRID, 
	S.SCC_ROW_ADD_DTTM, 
	S.SCC_ROW_UPD_OPRID, 
	S.SCC_ROW_UPD_DTTM, 
	'N', 
	'S', 
	sysdate, 
	sysdate, 
	1234)
	; 

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_SSR_STDNT_GRAD rows merged: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SSR_STDNT_GRAD',
      i_Action            => 'MERGE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Deleting', OLD_MAX_SCN = NEW_MAX_SCN
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD';

   strSqlCommand := 'commit';
   COMMIT;


   strMessage01 := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_SSR_STDNT_GRAD';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update DATA_ORIGIN on CSSTG_OWNER.PS_SSR_STDNT_GRAD';

   UPDATE CSSTG_OWNER.PS_SSR_STDNT_GRAD T
      SET T.DATA_ORIGIN = 'D', T.LASTUPD_EW_DTTM = SYSDATE
    WHERE T.DATA_ORIGIN <> 'D'
      AND EXISTS
                 (SELECT 1
                    FROM (SELECT EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, DEGREE
                            FROM CSSTG_OWNER.PS_SSR_STDNT_GRAD T2
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD') = 'Y'
                          MINUS
                          SELECT EMPLID, INSTITUTION, ACAD_CAREER, STDNT_CAR_NBR, ACAD_PROG, nvl(trim(EXP_GRAD_TERM),'-') EXP_GRAD_TERM, DEGREE
                            FROM SYSADM.PS_SSR_STDNT_GRAD@SASOURCE
                           WHERE (SELECT DELETE_FLG
                                    FROM CSSTG_OWNER.UM_STAGE_JOBS
                                   WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD') = 'Y') S
                   WHERE T.EMPLID = S.EMPLID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                     AND T.INSTITUTION = S.INSTITUTION
					 AND T.ACAD_CAREER = S.ACAD_CAREER
					 AND T.STDNT_CAR_NBR = S.STDNT_CAR_NBR
					 AND T.ACAD_PROG = S.ACAD_PROG
                     AND T.EXP_GRAD_TERM = S.EXP_GRAD_TERM
                     AND T.DEGREE = S.DEGREE
                     AND T.SRC_SYS_ID = 'CS90');

   strSqlCommand := 'SET intRowCount';
   intRowCount := SQL%ROWCOUNT;

   strSqlCommand := 'commit';
   COMMIT;

   strMessage01 :=
         '# of PS_SSR_STDNT_GRAD rows updated: '
      || TO_CHAR (intRowCount, '999,999,999,999');
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL (
      i_TargetTableName   => 'PS_SSR_STDNT_GRAD',
      i_Action            => 'UPDATE',
      i_RowCount          => intRowCount);


   strMessage01 := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);

   strSqlCommand := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

   UPDATE CSSTG_OWNER.UM_STAGE_JOBS
      SET TABLE_STATUS = 'Complete', END_DT = SYSDATE
    WHERE TABLE_NAME = 'PS_SSR_STDNT_GRAD';

   strSqlCommand := 'commit';
   COMMIT;


   strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
   COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

   strMessage01 := strProcessName || ' is complete.';
   COMMON_OWNER.SMT_LOG.PUT_MESSAGE (i_Message => strMessage01);
   
EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);
END PS_SSR_STDNT_GRAD_P;
/

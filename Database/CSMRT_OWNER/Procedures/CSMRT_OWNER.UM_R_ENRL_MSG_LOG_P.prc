CREATE OR REPLACE PROCEDURE             "UM_R_ENRL_MSG_LOG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_R_ENRL_MSG_LOG from PeopleSoft table UM_R_ENRL_MSG_LOG.
--
 --V01  SMT-xxxx 07/02/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_ENRL_MSG_LOG';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_R_ENRL_MSG_LOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_R_ENRL_MSG_LOG';

merge /*+ parallel(16) ENABLE_PARALLEL_DML */ into CSMRT_OWNER.UM_R_ENRL_MSG_LOG T
using (
	select ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ, MESSAGE_SEQ, SRC_SYS_ID, 
		   MESSAGE_SET_NBR, MESSAGE_NBR, MSG_SEVERITY, DTTM_STAMP_SEC, 
		   DATA_ORIGIN
	  from CSSTG_OWNER.PS_ENRL_MSG_LOG
	 where LASTUPD_EW_DTTM >= trunc(SYSDATE-365)
	) S
	   on (T.ENRL_REQUEST_ID = S.ENRL_REQUEST_ID
	  and  T.ENRL_REQ_DETL_SEQ = S.ENRL_REQ_DETL_SEQ
	  and  T.MESSAGE_SEQ = S.MESSAGE_SEQ
	  and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
	 when matched then update set 
	T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR,
	T.MESSAGE_NBR = S.MESSAGE_NBR,
	T.MSG_SEVERITY = S.MSG_SEVERITY,
	T.DTTM_STAMP_SEC = S.DTTM_STAMP_SEC,
	T.DATA_ORIGIN = S.DATA_ORIGIN,
	T.LASTUPD_EW_DTTM = SYSDATE
	where 
	decode(T.MESSAGE_SET_NBR,S.MESSAGE_SET_NBR,0,1) = 1 or
	decode(T.MESSAGE_NBR,S.MESSAGE_NBR,0,1) = 1 or
	decode(T.MSG_SEVERITY,S.MSG_SEVERITY,0,1) = 1 or
	decode(T.DTTM_STAMP_SEC,S.DTTM_STAMP_SEC,0,1) = 1 or
	decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
	 when not matched then
	insert (
	T.ENRL_REQUEST_ID,
	T.ENRL_REQ_DETL_SEQ,
	T.MESSAGE_SEQ,
	T.SRC_SYS_ID,
	T.MESSAGE_SET_NBR,
	T.MESSAGE_NBR,
	T.MSG_SEVERITY,
	T.DTTM_STAMP_SEC,
	T.DATA_ORIGIN,
	T.CREATED_EW_DTTM,
	T.LASTUPD_EW_DTTM)
	values (
	S.ENRL_REQUEST_ID,
	S.ENRL_REQ_DETL_SEQ,
	S.MESSAGE_SEQ,
	S.SRC_SYS_ID,
	S.MESSAGE_SET_NBR,
	S.MESSAGE_NBR,
	S.MSG_SEVERITY,
	S.DTTM_STAMP_SEC,
	S.DATA_ORIGIN,
	SYSDATE,
	SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_ENRL_MSG_LOG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_ENRL_MSG_LOG',
                i_Action            => 'MERGE',
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

END UM_R_ENRL_MSG_LOG_P;
/

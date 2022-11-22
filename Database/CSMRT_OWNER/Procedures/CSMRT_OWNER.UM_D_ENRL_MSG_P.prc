DROP PROCEDURE CSMRT_OWNER.UM_D_ENRL_MSG_P
/

--
-- UM_D_ENRL_MSG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_ENRL_MSG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--UM_D_ENRL_MSG  
--V01  SMT-xxxx 07/02/2018,    James Doucette
--                             Converted from SQL Script
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_ENRL_MSG';
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

strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_ENRL_MSG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_ENRL_MSG';

merge /*+ parallel(16) ENABLE_PARALLEL_DML */ into CSMRT_OWNER.UM_D_ENRL_MSG T
using (
	select MESSAGE_SET_NBR, MESSAGE_NBR, SRC_SYS_ID, 
		   MESSAGE_TEXT, MSG_SEVERITY, LAST_UPDATE_DTTM, DESCRLONG,
		   DATA_ORIGIN
	  from CSSTG_OWNER.PSMSGCATDEFN
	) S
	   on (T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR
	  and  T.MESSAGE_NBR = S.MESSAGE_NBR
	  and  T.SRC_SYS_ID = S.SRC_SYS_ID) 
	 when matched then update set 
	T.MESSAGE_TEXT = S.MESSAGE_TEXT,
	T.MSG_SEVERITY = S.MSG_SEVERITY,
	T.LAST_UPDATE_DTTM = S.LAST_UPDATE_DTTM,
	T.DESCRLONG = S.DESCRLONG, 
	T.DATA_ORIGIN = S.DATA_ORIGIN,
	T.LASTUPD_EW_DTTM = SYSDATE
	where 
	decode(T.MESSAGE_TEXT,S.MESSAGE_TEXT,0,1) = 1 or
	decode(T.MSG_SEVERITY,S.MSG_SEVERITY,0,1) = 1 or
	decode(T.LAST_UPDATE_DTTM,S.LAST_UPDATE_DTTM,0,1) = 1 or
	decode(T.DESCRLONG,S.DESCRLONG,0,1) = 1 or
	decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1 
	 when not matched then
	insert (
	T.MESSAGE_SET_NBR,
	T.MESSAGE_NBR,
	T.SRC_SYS_ID,
	T.MESSAGE_TEXT,
	T.MSG_SEVERITY,
	T.LAST_UPDATE_DTTM,
	T.DESCRLONG,
	T.DATA_ORIGIN,
	T.CREATED_EW_DTTM,
	T.LASTUPD_EW_DTTM)
	values (
	S.MESSAGE_SET_NBR,
	S.MESSAGE_NBR,
	S.SRC_SYS_ID,
	S.MESSAGE_TEXT,
	S.MSG_SEVERITY,
	S.LAST_UPDATE_DTTM,
	S.DESCRLONG,
	S.DATA_ORIGIN,
	SYSDATE,
	SYSDATE)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_ENRL_MSG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_ENRL_MSG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_ENRL_MSG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_ENRL_MSG';

update CSMRT_OWNER.UM_D_ENRL_MSG T 
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and not exists (select 1
                     from CSSTG_OWNER.PSMSGCATDEFN S    
                    where T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR
                      and T.MESSAGE_NBR = S.MESSAGE_NBR
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_ENRL_MSG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_ENRL_MSG',
                i_Action            => 'UPDATE',
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

END UM_D_ENRL_MSG_P;
/

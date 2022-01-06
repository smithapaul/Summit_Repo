CREATE OR REPLACE PROCEDURE             "BB_GROUPS_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_GROUPS_S2 from S1 table BB_GROUPS_S1.
--
-- V01  CASE-xxxxx 08/31/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_GROUPS_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_GROUPS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_GROUPS_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_GROUPS_S2 T
using (select /*+ full(S) */
       BB_SOURCE, 
       PK1, 
       CRSMAIN_PK1, 
       DTMODIFIED, 
       AVAILABLE_IND, 
       STUDENT_CREATE_FORUM_IND, 
       SET_PK1, 
       SET_IND, 
       SELF_ENROLL_IND, 
       SHOW_SELF_ENROLL_IND, 
       CUSTOMIZE_IND, 
       SIGNUP_IND, 
       ENROLL_LIMIT, 
       TEXT_FORMAT_TYPE, 
       GROUP_NAME, 
       TAB_PK1, 
       ALLOW_EDIT_TO_GROUP_IND, 
       BATCH_UID, 
       CREATE_COLLAB_RECORDINGS_IND, 
       UUID, 
       DELETE_FLAG, 
       INSERT_TIME, 
       UPDATE_TIME
  from CSSTG_OWNER.BB_GROUPS_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.CRSMAIN_PK1 = S.CRSMAIN_PK1,
    T.DTMODIFIED = S.DTMODIFIED,
	T.AVAILABLE_IND = S.AVAILABLE_IND,
	T.STUDENT_CREATE_FORUM_IND = S.STUDENT_CREATE_FORUM_IND,
	T.SET_PK1 = S.SET_PK1,
	T.SET_IND = S.SET_IND,
	T.SELF_ENROLL_IND = S.SELF_ENROLL_IND,
	T.SHOW_SELF_ENROLL_IND = S.SHOW_SELF_ENROLL_IND,
	T.CUSTOMIZE_IND = S.CUSTOMIZE_IND,
	T.SIGNUP_IND = S.SIGNUP_IND,
	T.ENROLL_LIMIT = S.ENROLL_LIMIT,
	T.TEXT_FORMAT_TYPE = S.TEXT_FORMAT_TYPE,
	T.GROUP_NAME = S.GROUP_NAME,
	T.TAB_PK1 = S.TAB_PK1,
	T.ALLOW_EDIT_TO_GROUP_IND = S.ALLOW_EDIT_TO_GROUP_IND,
	T.BATCH_UID = S.BATCH_UID,
	T.CREATE_COLLAB_RECORDINGS_IND = S.CREATE_COLLAB_RECORDINGS_IND,
	T.UUID = S.UUID,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(CRSMAIN_PK1),0) <> nvl(trim(S.CRSMAIN_PK1),0) or
    nvl(trim(DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or
    nvl(trim(AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or
    nvl(trim(STUDENT_CREATE_FORUM_IND),0) <> nvl(trim(S.STUDENT_CREATE_FORUM_IND),0) or
    nvl(trim(SET_PK1),0) <> nvl(trim(S.SET_PK1),0) or
    nvl(trim(SET_IND),0) <> nvl(trim(S.SET_IND),0) or
    nvl(trim(SELF_ENROLL_IND),0) <> nvl(trim(S.SELF_ENROLL_IND),0) or
    nvl(trim(SHOW_SELF_ENROLL_IND),0) <> nvl(trim(S.SHOW_SELF_ENROLL_IND),0) or
    nvl(trim(CUSTOMIZE_IND),0) <> nvl(trim(S.CUSTOMIZE_IND),0) or
    nvl(trim(SIGNUP_IND),0) <> nvl(trim(S.SIGNUP_IND),0) or
    nvl(trim(ENROLL_LIMIT),0) <> nvl(trim(S.ENROLL_LIMIT),0) or
    nvl(trim(TEXT_FORMAT_TYPE),0) <> nvl(trim(S.TEXT_FORMAT_TYPE),0) or
    nvl(trim(GROUP_NAME),0) <> nvl(trim(S.GROUP_NAME),0) or
    nvl(trim(TAB_PK1),0) <> nvl(trim(S.TAB_PK1),0) or
    nvl(trim(ALLOW_EDIT_TO_GROUP_IND),0) <> nvl(trim(S.ALLOW_EDIT_TO_GROUP_IND),0) or
    nvl(trim(BATCH_UID),0) <> nvl(trim(S.BATCH_UID),0) or
    nvl(trim(CREATE_COLLAB_RECORDINGS_IND),0) <> nvl(trim(S.CREATE_COLLAB_RECORDINGS_IND),0) or
    nvl(trim(UUID),0) <> nvl(trim(S.UUID),0) or
    nvl(trim(DELETE_FLAG),0) <> nvl(trim('N'),0) or
    nvl(trim(UPDATE_TIME),0) <> nvl(trim(SYSDATE),0)
when not matched then 
insert (
    T.BB_SOURCE, 
    T.PK1, 
    T.CRSMAIN_PK1, 
    T.DTMODIFIED, 
    T.AVAILABLE_IND, 
    T.STUDENT_CREATE_FORUM_IND, 
    T.SET_PK1, 
    T.SET_IND, 
    T.SELF_ENROLL_IND, 
    T.SHOW_SELF_ENROLL_IND, 
    T.CUSTOMIZE_IND, 
    T.SIGNUP_IND, 
    T.ENROLL_LIMIT, 
    T.TEXT_FORMAT_TYPE, 
    T.GROUP_NAME, 
    T.TAB_PK1, 
    T.ALLOW_EDIT_TO_GROUP_IND, 
    T.BATCH_UID, 
    T.CREATE_COLLAB_RECORDINGS_IND, 
    T.UUID,
    T.DELETE_FLAG, 
    T.INSERT_TIME, 
    T.UPDATE_TIME  
) 
values (
    S.BB_SOURCE, 
    S.PK1, 
    S.CRSMAIN_PK1, 
    S.DTMODIFIED, 
    S.AVAILABLE_IND, 
    S.STUDENT_CREATE_FORUM_IND, 
    S.SET_PK1, 
    S.SET_IND, 
    S.SELF_ENROLL_IND, 
    S.SHOW_SELF_ENROLL_IND, 
    S.CUSTOMIZE_IND, 
    S.SIGNUP_IND, 
    S.ENROLL_LIMIT, 
    S.TEXT_FORMAT_TYPE, 
    S.GROUP_NAME, 
    S.TAB_PK1, 
    S.ALLOW_EDIT_TO_GROUP_IND, 
    S.BATCH_UID, 
    S.CREATE_COLLAB_RECORDINGS_IND, 
    S.UUID,
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_GROUPS_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_GROUPS_S2',
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

END BB_GROUPS_S2_P;
/

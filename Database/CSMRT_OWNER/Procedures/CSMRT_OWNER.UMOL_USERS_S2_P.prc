DROP PROCEDURE CSMRT_OWNER.UMOL_USERS_S2_P
/

--
-- UMOL_USERS_S2_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UMOL_USERS_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table UMOL_USERS_S2 from S1 table UMOL_USERS_S1.
--
-- V01  CASE-xxxxx 08/31/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UMOL_USERS_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UMOL_USERS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UMOL_USERS_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.UMOL_USERS_S2 T
using (select /*+ full(S) */
       	BB_SOURCE, 
		PK1, 
		CITY, 
		DATA_SRC_PK1, 
		SYSTEM_ROLE, 
		SOS_ID_PK2, 
		DTCREATED, 
		DTMODIFIED, 
		ROW_STATUS, 
		BATCH_UID, 
		USER_ID, 
		PASSWD, 
		FIRSTNAME, 
		MIDDLENAME, 
		LASTNAME, 
		OTHERNAME, 
		SUFFIX, 
		GENDER, 
		EDUC_LEVEL, 
		BIRTHDATE, 
		TITLE, 
		STUDENT_ID, 
		EMAIL, 
		JOB_TITLE, 
		DEPARTMENT, 
		COMPANY, 
		STREET_1, 
		INSTITUTION_ROLES_PK1, 
		STREET_2, 
		STATE, 
		ZIP_CODE, 
		COUNTRY, 
		B_PHONE_1, 
		B_PHONE_2, 
		H_PHONE_1, 
		H_PHONE_2, 
		M_PHONE, 
		B_FAX, 
		H_FAX, 
		WEBPAGE, 
		COMMERCE_ROLE, 
		CDROMDRIVE_PC, 
		CDROMDRIVE_MAC, 
		PUBLIC_IND, 
		ADDRESS_IND, 
		PHONE_IND, 
		WORK_IND, 
		EMAIL_IND, 
		AVAILABLE_IND, 
		LAST_LOGIN_DATE, 
		IM_TYPE, 
		IM_ACCOUNT, 
		LOCALE, 
		CLD_ID, 
		CLD_AVATAR_URL, 
		UUID, 
		CALENDAR_TYPE, 
		WEEK_FIRST_DAY, 
		DELETE_FLAG, 
		INSERT_TIME, 
		UPDATE_TIME
  from CSSTG_OWNER.UMOL_USERS_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.CITY = S.CITY, 
    T.DATA_SRC_PK1 = S.DATA_SRC_PK1, 
    T.SYSTEM_ROLE = S.SYSTEM_ROLE, 
    T.SOS_ID_PK2 = S.SOS_ID_PK2, 
    T.DTCREATED = S.DTCREATED, 
    T.DTMODIFIED = S.DTMODIFIED, 
    T.ROW_STATUS = S.ROW_STATUS, 
    T.BATCH_UID = S.BATCH_UID, 
    T.USER_ID = S.USER_ID, 
    T.PASSWD = S.PASSWD, 
    T.FIRSTNAME = S.FIRSTNAME, 
    T.MIDDLENAME = S.MIDDLENAME, 
    T.LASTNAME = S.LASTNAME, 
    T.OTHERNAME = S.OTHERNAME, 
    T.SUFFIX = S.SUFFIX, 
    T.GENDER = S.GENDER, 
    T.EDUC_LEVEL = S.EDUC_LEVEL, 
    T.BIRTHDATE = S.BIRTHDATE, 
    T.TITLE = S.TITLE, 
    T.STUDENT_ID = S.STUDENT_ID, 
    T.EMAIL = S.EMAIL, 
    T.JOB_TITLE = S.JOB_TITLE, 
    T.DEPARTMENT = S.DEPARTMENT, 
    T.COMPANY = S.COMPANY, 
    T.STREET_1 = S.STREET_1, 
    T.INSTITUTION_ROLES_PK1 = S.INSTITUTION_ROLES_PK1, 
    T.STREET_2 = S.STREET_2, 
    T.STATE = S.STATE, 
    T.ZIP_CODE = S.ZIP_CODE, 
    T.COUNTRY = S.COUNTRY, 
    T.B_PHONE_1 = S.B_PHONE_1, 
    T.B_PHONE_2 = S.B_PHONE_2, 
    T.H_PHONE_1 = S.H_PHONE_1, 
    T.H_PHONE_2 = S.H_PHONE_2, 
    T.M_PHONE = S.M_PHONE, 
    T.B_FAX = S.B_FAX, 
    T.H_FAX = S.H_FAX, 
    T.WEBPAGE = S.WEBPAGE, 
    T.COMMERCE_ROLE = S.COMMERCE_ROLE, 
    T.CDROMDRIVE_PC = S.CDROMDRIVE_PC, 
    T.CDROMDRIVE_MAC = S.CDROMDRIVE_MAC, 
    T.PUBLIC_IND = S.PUBLIC_IND, 
    T.ADDRESS_IND = S.ADDRESS_IND, 
    T.PHONE_IND = S.PHONE_IND, 
    T.WORK_IND = S.WORK_IND, 
    T.EMAIL_IND = S.EMAIL_IND, 
    T.AVAILABLE_IND = S.AVAILABLE_IND, 
    T.LAST_LOGIN_DATE = S.LAST_LOGIN_DATE, 
    T.IM_TYPE = S.IM_TYPE, 
    T.IM_ACCOUNT = S.IM_ACCOUNT, 
    T.LOCALE = S.LOCALE, 
    T.CLD_ID = S.CLD_ID, 
    T.CLD_AVATAR_URL = S.CLD_AVATAR_URL, 
    T.UUID = S.UUID, 
    T.CALENDAR_TYPE = S.CALENDAR_TYPE, 
    T.WEEK_FIRST_DAY = S.WEEK_FIRST_DAY,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(T.CITY), 0) <> nvl(trim(S.CITY),0) or
    nvl(trim(T.DATA_SRC_PK1),0) <> nvl(trim(S.DATA_SRC_PK1),0) or 
    nvl(trim(T.SYSTEM_ROLE),0) <> nvl(trim(S.SYSTEM_ROLE),0) or 
    nvl(trim(T.SOS_ID_PK2),0) <> nvl(trim(S.SOS_ID_PK2),0) or 
    nvl(trim(T.DTCREATED),0) <> nvl(trim(S.DTCREATED),0) or 
    nvl(trim(T.DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or 
    nvl(trim(T.ROW_STATUS),0) <> nvl(trim(S.ROW_STATUS),0) or 
    nvl(trim(T.BATCH_UID),0) <> nvl(trim(S.BATCH_UID),0) or 
    nvl(trim(T.USER_ID),0) <> nvl(trim(S.USER_ID),0) or 
    nvl(trim(T.PASSWD),0) <> nvl(trim(S.PASSWD),0) or 
    nvl(trim(T.FIRSTNAME),0) <> nvl(trim(S.FIRSTNAME),0) or 
    nvl(trim(T.MIDDLENAME),0) <> nvl(trim(S.MIDDLENAME),0) or 
    nvl(trim(T.LASTNAME),0) <> nvl(trim(S.LASTNAME),0) or 
    nvl(trim(T.OTHERNAME),0) <> nvl(trim(S.OTHERNAME),0) or 
    nvl(trim(T.SUFFIX),0) <> nvl(trim(S.SUFFIX),0) or 
    nvl(trim(T.GENDER),0) <> nvl(trim(S.GENDER),0) or 
    nvl(trim(T.EDUC_LEVEL),0) <> nvl(trim(S.EDUC_LEVEL),0) or 
    nvl(trim(T.BIRTHDATE),0) <> nvl(trim(S.BIRTHDATE),0) or 
    nvl(trim(T.TITLE),0) <> nvl(trim(S.TITLE),0) or 
    nvl(trim(T.STUDENT_ID),0) <> nvl(trim(S.STUDENT_ID),0) or 
    nvl(trim(T.EMAIL),0) <> nvl(trim(S.EMAIL),0) or 
    nvl(trim(T.JOB_TITLE),0) <> nvl(trim(S.JOB_TITLE),0) or 
    nvl(trim(T.DEPARTMENT),0) <> nvl(trim(S.DEPARTMENT),0) or 
    nvl(trim(T.COMPANY),0) <> nvl(trim(S.COMPANY),0) or 
    nvl(trim(T.STREET_1),0) <> nvl(trim(S.STREET_1),0) or 
    nvl(trim(T.INSTITUTION_ROLES_PK1),0) <> nvl(trim(S.INSTITUTION_ROLES_PK1),0) or 
    nvl(trim(T.STREET_2),0) <> nvl(trim(S.STREET_2),0) or 
    nvl(trim(T.STATE),0) <> nvl(trim(S.STATE),0) or 
    nvl(trim(T.ZIP_CODE),0) <> nvl(trim(S.ZIP_CODE),0) or 
    nvl(trim(T.COUNTRY),0) <> nvl(trim(S.COUNTRY),0) or 
    nvl(trim(T.B_PHONE_1),0) <> nvl(trim(S.B_PHONE_1),0) or 
    nvl(trim(T.B_PHONE_2),0) <> nvl(trim(S.B_PHONE_2),0) or 
    nvl(trim(T.H_PHONE_1),0) <> nvl(trim(S.H_PHONE_1),0) or 
    nvl(trim(T.H_PHONE_2),0) <> nvl(trim(S.H_PHONE_2),0) or 
    nvl(trim(T.M_PHONE),0) <> nvl(trim(S.M_PHONE),0) or 
    nvl(trim(T.B_FAX),0) <> nvl(trim(S.B_FAX),0) or 
    nvl(trim(T.H_FAX),0) <> nvl(trim(S.H_FAX),0) or 
    nvl(trim(T.WEBPAGE),0) <> nvl(trim(S.WEBPAGE),0) or 
    nvl(trim(T.COMMERCE_ROLE),0) <> nvl(trim(S.COMMERCE_ROLE),0) or 
    nvl(trim(T.CDROMDRIVE_PC),0) <> nvl(trim(S.CDROMDRIVE_PC),0) or 
    nvl(trim(T.CDROMDRIVE_MAC),0) <> nvl(trim(S.CDROMDRIVE_MAC),0) or 
    nvl(trim(T.PUBLIC_IND),0) <> nvl(trim(S.PUBLIC_IND),0) or 
    nvl(trim(T.ADDRESS_IND),0) <> nvl(trim(S.ADDRESS_IND),0) or 
    nvl(trim(T.PHONE_IND),0) <> nvl(trim(S.PHONE_IND),0) or 
    nvl(trim(T.WORK_IND),0) <> nvl(trim(S.WORK_IND),0) or 
    nvl(trim(T.EMAIL_IND),0) <> nvl(trim(S.EMAIL_IND),0) or 
    nvl(trim(T.AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or 
    nvl(trim(T.LAST_LOGIN_DATE),0) <> nvl(trim(S.LAST_LOGIN_DATE),0) or 
    nvl(trim(T.IM_TYPE),0) <> nvl(trim(S.IM_TYPE),0) or 
    nvl(trim(T.IM_ACCOUNT),0) <> nvl(trim(S.IM_ACCOUNT),0) or 
    nvl(trim(T.LOCALE),0) <> nvl(trim(S.LOCALE),0) or 
    nvl(trim(T.CLD_ID),0) <> nvl(trim(S.CLD_ID),0) or 
    nvl(trim(T.CLD_AVATAR_URL),0) <> nvl(trim(S.CLD_AVATAR_URL),0) or 
    nvl(trim(T.UUID),0) <> nvl(trim(S.UUID),0) or 
    nvl(trim(T.CALENDAR_TYPE),0) <> nvl(trim(S.CALENDAR_TYPE),0) or 
    nvl(trim(T.WEEK_FIRST_DAY),0) <> nvl(trim(S.WEEK_FIRST_DAY),0)
when not matched then 
insert (
    T.BB_SOURCE, 
    T.PK1, 
    T.CITY, 
    T.DATA_SRC_PK1, 
    T.SYSTEM_ROLE, 
    T.SOS_ID_PK2, 
    T.DTCREATED, 
    T.DTMODIFIED, 
    T.ROW_STATUS, 
    T.BATCH_UID, 
    T.USER_ID, 
    T.PASSWD, 
    T.FIRSTNAME, 
    T.MIDDLENAME, 
    T.LASTNAME, 
    T.OTHERNAME, 
    T.SUFFIX, 
    T.GENDER, 
    T.EDUC_LEVEL, 
    T.BIRTHDATE, 
    T.TITLE, 
    T.STUDENT_ID, 
    T.EMAIL, 
    T.JOB_TITLE, 
    T.DEPARTMENT, 
    T.COMPANY, 
    T.STREET_1, 
    T.INSTITUTION_ROLES_PK1, 
    T.STREET_2, 
    T.STATE, 
    T.ZIP_CODE, 
    T.COUNTRY, 
    T.B_PHONE_1, 
    T.B_PHONE_2, 
    T.H_PHONE_1, 
    T.H_PHONE_2, 
    T.M_PHONE, 
    T.B_FAX, 
    T.H_FAX, 
    T.WEBPAGE, 
    T.COMMERCE_ROLE, 
    T.CDROMDRIVE_PC, 
    T.CDROMDRIVE_MAC, 
    T.PUBLIC_IND, 
    T.ADDRESS_IND, 
    T.PHONE_IND, 
    T.WORK_IND, 
    T.EMAIL_IND, 
    T.AVAILABLE_IND, 
    T.LAST_LOGIN_DATE, 
    T.IM_TYPE, 
    T.IM_ACCOUNT, 
    T.LOCALE, 
    T.CLD_ID, 
    T.CLD_AVATAR_URL, 
    T.UUID, 
    T.CALENDAR_TYPE, 
    T.WEEK_FIRST_DAY,
    T.DELETE_FLAG, 
    T.INSERT_TIME, 
    T.UPDATE_TIME  
) 
values (
    S.BB_SOURCE, 
    S.PK1, 
    S.CITY, 
    S.DATA_SRC_PK1, 
    S.SYSTEM_ROLE, 
    S.SOS_ID_PK2, 
    S.DTCREATED, 
    S.DTMODIFIED, 
    S.ROW_STATUS, 
    S.BATCH_UID, 
    S.USER_ID, 
    S.PASSWD, 
    S.FIRSTNAME, 
    S.MIDDLENAME, 
    S.LASTNAME, 
    S.OTHERNAME, 
    S.SUFFIX, 
    S.GENDER, 
    S.EDUC_LEVEL, 
    S.BIRTHDATE, 
    S.TITLE, 
    S.STUDENT_ID, 
    S.EMAIL, 
    S.JOB_TITLE, 
    S.DEPARTMENT, 
    S.COMPANY, 
    S.STREET_1, 
    S.INSTITUTION_ROLES_PK1, 
    S.STREET_2, 
    S.STATE, 
    S.ZIP_CODE, 
    S.COUNTRY, 
    S.B_PHONE_1, 
    S.B_PHONE_2, 
    S.H_PHONE_1, 
    S.H_PHONE_2, 
    S.M_PHONE, 
    S.B_FAX, 
    S.H_FAX, 
    S.WEBPAGE, 
    S.COMMERCE_ROLE, 
    S.CDROMDRIVE_PC, 
    S.CDROMDRIVE_MAC, 
    S.PUBLIC_IND, 
    S.ADDRESS_IND, 
    S.PHONE_IND, 
    S.WORK_IND, 
    S.EMAIL_IND, 
    S.AVAILABLE_IND, 
    S.LAST_LOGIN_DATE, 
    S.IM_TYPE, 
    S.IM_ACCOUNT, 
    S.LOCALE, 
    S.CLD_ID, 
    S.CLD_AVATAR_URL, 
    S.UUID, 
    S.CALENDAR_TYPE, 
    S.WEEK_FIRST_DAY,  
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UMOL_USERS_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UMOL_USERS_S2',
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

END UMOL_USERS_S2_P;
/

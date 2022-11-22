DROP PROCEDURE CSMRT_OWNER.UMOL_COURSE_MAIN_S2_P
/

--
-- UMOL_COURSE_MAIN_S2_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UMOL_COURSE_MAIN_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table UMOL_COURSE_MAIN_S2 from S1 table UMOL_COURSE_MAIN_S1.
--
-- V01  CASE-xxxxx 08/31/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UMOL_COURSE_MAIN_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UMOL_COURSE_MAIN_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UMOL_COURSE_MAIN_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.UMOL_COURSE_MAIN_S2 T
using (select /*+ full(S) */
        BB_SOURCE, 
		PK1, 
		BUTTONSTYLES_PK1, 
		CARTRIDGE_PK1, 
		CLASSIFICATIONS_PK1, 
		DATA_SRC_PK1, 
		SOS_ID_PK2, 
		DTCREATED, 
		DTMODIFIED, 
		COURSE_NAME, 
		COURSE_ID, 
		LOWER_COURSE_ID, 
		ROW_STATUS, 
		BATCH_UID, 
		ENROLL_OPTION, 
		DURATION, 
		PACE, 
		SERVICE_LEVEL, 
		ABS_LIMIT, 
		SOFT_LIMIT, 
		UPLOAD_LIMIT, 
		START_DATE, 
		END_DATE, 
		ENROLL_START_DATE, 
		ENROLL_END_DATE, 
		DAYS_OF_USE, 
		FEE, 
		ENROLL_ACCESS_CODE, 
		BANNER_URL, 
		INSTITUTION_NAME, 
		REG_LEVEL_IND, 
		NAVIGATION_STYLE, 
		TEXTCOLOR, 
		BACKGROUND_COLOR, 
		COLLAPSIBLE_IND, 
		ALLOW_GUEST_IND, 
		CATALOG_IND, 
		LOCKOUT_IND, 
		DESC_PAGE_IND, 
		AVAILABLE_IND, 
		ALLOW_OBSERVER_IND, 
		DEFAULT_CONTENT_VIEW, 
		LOCALE, 
		IS_LOCALE_ENFORCED, 
		ASMT_UPGRADE_VERSION, 
		ASMT_UPGRADE_FLAGS, 
		HONOR_TERM_AVAIL_IND, 
		COURSE_THEME_PK1, 
		IMPORT_TASK_PK1, 
		UUID, 
		ULTRA_STATUS, 
		COURSE_VIEW_OPTION, 
		IS_CLOSED_IND, 
		CONVERT_TASK_PK1, 
		BANNER_ALT, 
		COPY_FROM_UUID
  from CSSTG_OWNER.UMOL_COURSE_MAIN_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.BUTTONSTYLES_PK1 = S.BUTTONSTYLES_PK1,
    T.CARTRIDGE_PK1 = S.CARTRIDGE_PK1,
    T.CLASSIFICATIONS_PK1 = S.CLASSIFICATIONS_PK1,
    T.DATA_SRC_PK1 = S.DATA_SRC_PK1,
    T.SOS_ID_PK2 = S.SOS_ID_PK2,
    T.DTCREATED = S.DTCREATED,
    T.DTMODIFIED = S.DTMODIFIED,
	T.COURSE_NAME = S.COURSE_NAME,
	T.COURSE_ID = S.COURSE_ID,
	T.LOWER_COURSE_ID = S.LOWER_COURSE_ID,
	T.ROW_STATUS = S.ROW_STATUS,
	T.BATCH_UID = S.BATCH_UID,
	T.ENROLL_OPTION = S.ENROLL_OPTION,
	T.DURATION = S.DURATION,
	T.PACE = S.PACE,
	T.SERVICE_LEVEL = S.SERVICE_LEVEL,
	T.ABS_LIMIT = S.ABS_LIMIT,
	T.SOFT_LIMIT = S.SOFT_LIMIT,
	T.UPLOAD_LIMIT = S.UPLOAD_LIMIT,
	T.START_DATE = S.START_DATE,
	T.END_DATE = S.END_DATE,
	T.ENROLL_START_DATE = S.ENROLL_START_DATE,
	T.ENROLL_END_DATE = S.ENROLL_END_DATE,
	T.DAYS_OF_USE = S.DAYS_OF_USE,
	T.FEE = S.FEE,
	T.ENROLL_ACCESS_CODE = S.ENROLL_ACCESS_CODE,
	T.BANNER_URL = S.BANNER_URL,
	T.INSTITUTION_NAME = S.INSTITUTION_NAME,
	T.REG_LEVEL_IND = S.REG_LEVEL_IND,
	T.NAVIGATION_STYLE = S.NAVIGATION_STYLE,
	T.TEXTCOLOR = S.TEXTCOLOR,
	T.BACKGROUND_COLOR = S.BACKGROUND_COLOR,
	T.COLLAPSIBLE_IND = S.COLLAPSIBLE_IND,
	T.ALLOW_GUEST_IND = S.ALLOW_GUEST_IND,
	T.CATALOG_IND = S.CATALOG_IND,
	T.LOCKOUT_IND = S.LOCKOUT_IND,
	T.DESC_PAGE_IND = S.DESC_PAGE_IND,
	T.AVAILABLE_IND = S.AVAILABLE_IND,
	T.ALLOW_OBSERVER_IND = S.ALLOW_OBSERVER_IND,
	T.DEFAULT_CONTENT_VIEW = S.DEFAULT_CONTENT_VIEW,
	T.LOCALE = S.LOCALE,
	T.IS_LOCALE_ENFORCED = S.IS_LOCALE_ENFORCED,
	T.ASMT_UPGRADE_VERSION = S.ASMT_UPGRADE_VERSION,
	T.ASMT_UPGRADE_FLAGS = S.ASMT_UPGRADE_FLAGS,
	T.HONOR_TERM_AVAIL_IND = S.HONOR_TERM_AVAIL_IND,
	T.COURSE_THEME_PK1 = S.COURSE_THEME_PK1,
	T.IMPORT_TASK_PK1 = S.IMPORT_TASK_PK1,
	T.UUID = S.UUID,
	T.ULTRA_STATUS = S.ULTRA_STATUS,
	T.COURSE_VIEW_OPTION = S.COURSE_VIEW_OPTION,
	T.IS_CLOSED_IND = S.IS_CLOSED_IND,
	T.CONVERT_TASK_PK1 = S.CONVERT_TASK_PK1,
	T.BANNER_ALT = S.BANNER_ALT,
	T.COPY_FROM_UUID = S.COPY_FROM_UUID,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(BUTTONSTYLES_PK1),0) <> nvl(trim(S.BUTTONSTYLES_PK1),0) or
    nvl(trim(CARTRIDGE_PK1),0) <> nvl(trim(S.CARTRIDGE_PK1),0) or
    nvl(trim(CLASSIFICATIONS_PK1),0) <> nvl(trim(S.CLASSIFICATIONS_PK1),0) or
    nvl(trim(DATA_SRC_PK1),0) <> nvl(trim(S.DATA_SRC_PK1),0) or
    nvl(trim(SOS_ID_PK2),0) <> nvl(trim(S.SOS_ID_PK2),0) or
    nvl(trim(DTCREATED),0) <> nvl(trim(S.DTCREATED),0) or
    nvl(trim(DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or
	nvl(trim(COURSE_NAME),0) <> nvl(trim(S.COURSE_NAME),0) or
	nvl(trim(COURSE_ID),0) <> nvl(trim(S.COURSE_ID),0) or
	nvl(trim(LOWER_COURSE_ID),0) <> nvl(trim(S.LOWER_COURSE_ID),0) or
	nvl(trim(ROW_STATUS),0) <> nvl(trim(S.ROW_STATUS),0) or
	nvl(trim(BATCH_UID),0) <> nvl(trim(S.BATCH_UID),0) or
	nvl(trim(ENROLL_OPTION),0) <> nvl(trim(S.ENROLL_OPTION),0) or
	nvl(trim(DURATION),0) <> nvl(trim(S.DURATION),0) or
	nvl(trim(PACE),0) <> nvl(trim(S.PACE),0) or
	nvl(trim(SERVICE_LEVEL),0) <> nvl(trim(S.SERVICE_LEVEL),0) or
	nvl(trim(ABS_LIMIT),0) <> nvl(trim(S.ABS_LIMIT),0) or
	nvl(trim(SOFT_LIMIT),0) <> nvl(trim(S.SOFT_LIMIT),0) or
	nvl(trim(UPLOAD_LIMIT),0) <> nvl(trim(S.UPLOAD_LIMIT),0) or
	nvl(trim(START_DATE),0) <> nvl(trim(S.START_DATE),0) or
	nvl(trim(END_DATE),0) <> nvl(trim(S.END_DATE),0) or
	nvl(trim(ENROLL_START_DATE),0) <> nvl(trim(S.ENROLL_START_DATE),0) or
	nvl(trim(ENROLL_END_DATE),0) <> nvl(trim(S.ENROLL_END_DATE),0) or
	nvl(trim(DAYS_OF_USE),0) <> nvl(trim(S.DAYS_OF_USE),0) or
	nvl(trim(FEE),0) <> nvl(trim(S.FEE),0) or
	nvl(trim(ENROLL_ACCESS_CODE),0) <> nvl(trim(S.ENROLL_ACCESS_CODE),0) or
	nvl(trim(BANNER_URL),0) <> nvl(trim(S.BANNER_URL),0) or
	nvl(trim(INSTITUTION_NAME),0) <> nvl(trim(S.INSTITUTION_NAME),0) or
	nvl(trim(REG_LEVEL_IND),0) <> nvl(trim(S.REG_LEVEL_IND),0) or
	nvl(trim(NAVIGATION_STYLE),0) <> nvl(trim(S.NAVIGATION_STYLE),0) or
	nvl(trim(TEXTCOLOR),0) <> nvl(trim(S.TEXTCOLOR),0) or
	nvl(trim(BACKGROUND_COLOR),0) <> nvl(trim(S.BACKGROUND_COLOR),0) or
	nvl(trim(COLLAPSIBLE_IND),0) <> nvl(trim(S.COLLAPSIBLE_IND),0) or
	nvl(trim(ALLOW_GUEST_IND),0) <> nvl(trim(S.ALLOW_GUEST_IND),0) or
	nvl(trim(CATALOG_IND),0) <> nvl(trim(S.CATALOG_IND),0) or
	nvl(trim(LOCKOUT_IND),0) <> nvl(trim(S.LOCKOUT_IND),0) or
	nvl(trim(DESC_PAGE_IND),0) <> nvl(trim(S.DESC_PAGE_IND),0) or
	nvl(trim(AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or
	nvl(trim(ALLOW_OBSERVER_IND),0) <> nvl(trim(S.ALLOW_OBSERVER_IND),0) or
	nvl(trim(DEFAULT_CONTENT_VIEW),0) <> nvl(trim(S.DEFAULT_CONTENT_VIEW),0) or
	nvl(trim(LOCALE),0) <> nvl(trim(S.LOCALE),0) or
	nvl(trim(IS_LOCALE_ENFORCED),0) <> nvl(trim(S.IS_LOCALE_ENFORCED),0) or
	nvl(trim(ASMT_UPGRADE_VERSION),0) <> nvl(trim(S.ASMT_UPGRADE_VERSION),0) or
	nvl(trim(ASMT_UPGRADE_FLAGS),0) <> nvl(trim(S.ASMT_UPGRADE_FLAGS),0) or
	nvl(trim(HONOR_TERM_AVAIL_IND),0) <> nvl(trim(S.HONOR_TERM_AVAIL_IND),0) or
	nvl(trim(COURSE_THEME_PK1),0) <> nvl(trim(S.COURSE_THEME_PK1),0) or
	nvl(trim(IMPORT_TASK_PK1),0) <> nvl(trim(S.IMPORT_TASK_PK1),0) or
	nvl(trim(UUID),0) <> nvl(trim(S.UUID),0) or
	nvl(trim(ULTRA_STATUS),0) <> nvl(trim(S.ULTRA_STATUS),0) or
	nvl(trim(COURSE_VIEW_OPTION),0) <> nvl(trim(S.COURSE_VIEW_OPTION),0) or
	nvl(trim(IS_CLOSED_IND),0) <> nvl(trim(S.IS_CLOSED_IND),0) or
	nvl(trim(CONVERT_TASK_PK1),0) <> nvl(trim(S.CONVERT_TASK_PK1),0) or
	nvl(trim(BANNER_ALT),0) <> nvl(trim(S.BANNER_ALT),0) or
	nvl(trim(COPY_FROM_UUID),0) <> nvl(trim(S.COPY_FROM_UUID),0)	
when not matched then 
insert (
    T.BB_SOURCE, 
    T.PK1, 
    T.BUTTONSTYLES_PK1, 
    T.CARTRIDGE_PK1, 
    T.CLASSIFICATIONS_PK1, 
    T.DATA_SRC_PK1, 
    T.SOS_ID_PK2, 
    T.DTCREATED, 
    T.DTMODIFIED, 
    T.COURSE_NAME, 
    T.COURSE_ID, 
    T.LOWER_COURSE_ID, 
    T.ROW_STATUS, 
    T.BATCH_UID, 
    T.ENROLL_OPTION, 
    T.DURATION, 
    T.PACE, 
    T.SERVICE_LEVEL, 
    T.ABS_LIMIT, 
    T.SOFT_LIMIT, 
    T.UPLOAD_LIMIT, 
    T.START_DATE, 
    T.END_DATE, 
    T.ENROLL_START_DATE, 
    T.ENROLL_END_DATE, 
    T.DAYS_OF_USE, 
    T.FEE, 
    T.ENROLL_ACCESS_CODE, 
    T.BANNER_URL, 
    T.INSTITUTION_NAME, 
    T.REG_LEVEL_IND, 
    T.NAVIGATION_STYLE, 
    T.TEXTCOLOR, 
    T.BACKGROUND_COLOR, 
    T.COLLAPSIBLE_IND, 
    T.ALLOW_GUEST_IND, 
    T.CATALOG_IND, 
    T.LOCKOUT_IND, 
    T.DESC_PAGE_IND, 
    T.AVAILABLE_IND, 
    T.ALLOW_OBSERVER_IND, 
    T.DEFAULT_CONTENT_VIEW, 
    T.LOCALE, 
    T.IS_LOCALE_ENFORCED, 
    T.ASMT_UPGRADE_VERSION, 
    T.ASMT_UPGRADE_FLAGS, 
    T.HONOR_TERM_AVAIL_IND, 
    T.COURSE_THEME_PK1, 
    T.IMPORT_TASK_PK1, 
    T.UUID, 
    T.ULTRA_STATUS, 
    T.COURSE_VIEW_OPTION, 
    T.IS_CLOSED_IND, 
    T.CONVERT_TASK_PK1, 
    T.BANNER_ALT, 
    T.COPY_FROM_UUID, 
    T.DELETE_FLAG, 
    T.INSERT_TIME, 
    T.UPDATE_TIME  
) 
values (
    S.BB_SOURCE, 
    S.PK1, 
    S.BUTTONSTYLES_PK1, 
    S.CARTRIDGE_PK1, 
    S.CLASSIFICATIONS_PK1, 
    S.DATA_SRC_PK1, 
    S.SOS_ID_PK2, 
    S.DTCREATED, 
    S.DTMODIFIED, 
    S.COURSE_NAME, 
    S.COURSE_ID, 
    S.LOWER_COURSE_ID, 
    S.ROW_STATUS, 
    S.BATCH_UID, 
    S.ENROLL_OPTION, 
    S.DURATION, 
    S.PACE, 
    S.SERVICE_LEVEL, 
    S.ABS_LIMIT, 
    S.SOFT_LIMIT, 
    S.UPLOAD_LIMIT, 
    S.START_DATE, 
    S.END_DATE, 
    S.ENROLL_START_DATE, 
    S.ENROLL_END_DATE, 
    S.DAYS_OF_USE, 
    S.FEE, 
    S.ENROLL_ACCESS_CODE, 
    S.BANNER_URL, 
    S.INSTITUTION_NAME, 
    S.REG_LEVEL_IND, 
    S.NAVIGATION_STYLE, 
    S.TEXTCOLOR, 
    S.BACKGROUND_COLOR, 
    S.COLLAPSIBLE_IND, 
    S.ALLOW_GUEST_IND, 
    S.CATALOG_IND, 
    S.LOCKOUT_IND, 
    S.DESC_PAGE_IND, 
    S.AVAILABLE_IND, 
    S.ALLOW_OBSERVER_IND, 
    S.DEFAULT_CONTENT_VIEW, 
    S.LOCALE, 
    S.IS_LOCALE_ENFORCED, 
    S.ASMT_UPGRADE_VERSION, 
    S.ASMT_UPGRADE_FLAGS, 
    S.HONOR_TERM_AVAIL_IND, 
    S.COURSE_THEME_PK1, 
    S.IMPORT_TASK_PK1, 
    S.UUID, 
    S.ULTRA_STATUS, 
    S.COURSE_VIEW_OPTION, 
    S.IS_CLOSED_IND, 
    S.CONVERT_TASK_PK1, 
    S.BANNER_ALT, 
    S.COPY_FROM_UUID, 
    'N',          
    SYSDATE,          
    SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UMOL_COURSE_MAIN_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UMOL_COURSE_MAIN_S2',
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

END UMOL_COURSE_MAIN_S2_P;
/

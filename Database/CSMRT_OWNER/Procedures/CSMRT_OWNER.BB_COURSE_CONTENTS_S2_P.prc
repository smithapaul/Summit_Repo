CREATE OR REPLACE PROCEDURE             "BB_COURSE_CONTENTS_S2_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- Jim Doucette
--
-- Loads S2 table BB_COURSE_CONTENTS_S2 from S1 table BB_CLASSIFICATIONS_S1.
--
-- V01  CASE-xxxxx 08/28/2020,    Jim Doucette
--                                New
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'BB_COURSE_CONTENTS_S2';
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

strMessage01    := 'Merging data into CSSTG_OWNER.BB_COURSE_CONTENTS_S2';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.BB_COURSE_CONTENTS_S2';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.BB_COURSE_CONTENTS_S2 T
using (select /*+ full(S) */
              BB_SOURCE, PK1, 
			  CNTHNDLR_HANDLE, DTCREATED, DTMODIFIED, CONTENT_TYPE, POSITION, 
			  FONT_COLOR, TEXT_FORMAT_TYPE, OFFLINE_NAME, OFFLINE_PATH, START_DATE, 
			  CRSMAIN_PK1, END_DATE, LESSON_IND, SEQUENTIAL_IND, NEW_WINDOW_IND, 
			  TRACKING_IND, FOLDER_IND, DESCRIBE_IND, CARTRIDGE_IND, AVAILABLE_IND, 
			  WEB_URL, WEB_URL_HOST, ALLOW_GUEST_IND, ALLOW_OBSERVER_IND, 
			  IS_GROUP_CONTENT, TITLE, PARENT_PK1, DATA_VERSION, REVIEWABLE_IND, 
			  VIEW_MODE, LINK_REF, SAMPLE_CONTENT_IND, PARTIALLY_VISIBLE_IND, 
			  DESCRIPTION, FOLDER_TYPE, COPY_FROM_PK1
  from CSSTG_OWNER.BB_COURSE_CONTENTS_S1) S
 on ( 
    T.BB_SOURCE = S.BB_SOURCE and 
    T.PK1 = S.PK1)
 when matched then update set
    T.CNTHNDLR_HANDLE = S.CNTHNDLR_HANDLE,
    T.DTCREATED = S.DTCREATED,
    T.DTMODIFIED = S.DTMODIFIED,
    T.CONTENT_TYPE = S.CONTENT_TYPE,
    T.POSITION = S.POSITION,
    T.FONT_COLOR = S.FONT_COLOR,
    T.TEXT_FORMAT_TYPE = S.TEXT_FORMAT_TYPE,
    T.OFFLINE_NAME = S.OFFLINE_NAME,
	T.OFFLINE_PATH = S.OFFLINE_PATH,
	T.START_DATE = S.START_DATE,
	T.CRSMAIN_PK1 = S.CRSMAIN_PK1,
	T.END_DATE = S.END_DATE,
	T.LESSON_IND = S.LESSON_IND,
	T.SEQUENTIAL_IND = S.SEQUENTIAL_IND,
	T.NEW_WINDOW_IND = S.NEW_WINDOW_IND,
	T.TRACKING_IND = S.TRACKING_IND,
	T.FOLDER_IND = S.FOLDER_IND,
	T.DESCRIBE_IND = S.DESCRIBE_IND,
	T.CARTRIDGE_IND = S.CARTRIDGE_IND,
	T.AVAILABLE_IND = S.AVAILABLE_IND,
	T.WEB_URL = S.WEB_URL,
	T.WEB_URL_HOST = S.WEB_URL_HOST,
	T.ALLOW_GUEST_IND = S.ALLOW_GUEST_IND,
	T.ALLOW_OBSERVER_IND = S.ALLOW_OBSERVER_IND,
	T.IS_GROUP_CONTENT = S.IS_GROUP_CONTENT,
	T.TITLE = S.TITLE,
	T.PARENT_PK1 = S.PARENT_PK1,
	T.DATA_VERSION = S.DATA_VERSION,
	T.REVIEWABLE_IND = S.REVIEWABLE_IND,
	T.VIEW_MODE = S.VIEW_MODE,
	T.LINK_REF = S.LINK_REF,
	T.SAMPLE_CONTENT_IND = S.SAMPLE_CONTENT_IND,
	T.PARTIALLY_VISIBLE_IND = S.PARTIALLY_VISIBLE_IND,
	T.DESCRIPTION = S.DESCRIPTION,
	T.FOLDER_TYPE = S.FOLDER_TYPE,
	T.COPY_FROM_PK1 = S.COPY_FROM_PK1,
	T.DELETE_FLAG = 'N',
	T.UPDATE_TIME = SYSDATE
where 
    nvl(trim(T.CNTHNDLR_HANDLE),0) <> nvl(trim(S.CNTHNDLR_HANDLE),0) or 
    nvl(trim(T.DTCREATED),0) <> nvl(trim(S.DTCREATED),0) or 
    nvl(trim(T.DTMODIFIED),0) <> nvl(trim(S.DTMODIFIED),0) or 
    nvl(trim(T.CONTENT_TYPE),0) <> nvl(trim(S.CONTENT_TYPE),0) or 
    nvl(trim(T.POSITION),0) <> nvl(trim(S.POSITION),0) or 
    nvl(trim(T.FONT_COLOR),0) <> nvl(trim(S.FONT_COLOR),0) or 
    nvl(trim(T.TEXT_FORMAT_TYPE),0) <> nvl(trim(S.TEXT_FORMAT_TYPE),0) or 
    nvl(trim(T.OFFLINE_NAME),0) <> nvl(trim(S.OFFLINE_NAME),0) or
	nvl(trim(T.OFFLINE_PATH),0) <> nvl(trim(S.OFFLINE_PATH),0) or 
    nvl(trim(T.START_DATE),0) <> nvl(trim(S.START_DATE),0) or 
    nvl(trim(T.CRSMAIN_PK1),0) <> nvl(trim(S.CRSMAIN_PK1),0) or 
    nvl(trim(T.END_DATE),0) <> nvl(trim(S.END_DATE),0) or 
    nvl(trim(T.LESSON_IND),0) <> nvl(trim(S.LESSON_IND),0) or 
    nvl(trim(T.SEQUENTIAL_IND),0) <> nvl(trim(S.SEQUENTIAL_IND),0) or 
    nvl(trim(T.NEW_WINDOW_IND),0) <> nvl(trim(S.NEW_WINDOW_IND),0) or 
    nvl(trim(T.TRACKING_IND),0) <> nvl(trim(S.TRACKING_IND),0) or
	nvl(trim(T.FOLDER_IND),0) <> nvl(trim(S.FOLDER_IND),0) or 
    nvl(trim(T.DESCRIBE_IND),0) <> nvl(trim(S.DESCRIBE_IND),0) or 
    nvl(trim(T.CARTRIDGE_IND),0) <> nvl(trim(S.CARTRIDGE_IND),0) or 
    nvl(trim(T.AVAILABLE_IND),0) <> nvl(trim(S.AVAILABLE_IND),0) or 
    nvl(trim(T.WEB_URL),0) <> nvl(trim(S.WEB_URL),0) or 
    nvl(trim(T.WEB_URL_HOST),0) <> nvl(trim(S.WEB_URL_HOST),0) or 
    nvl(trim(T.ALLOW_GUEST_IND),0) <> nvl(trim(S.ALLOW_GUEST_IND),0) or 
    nvl(trim(T.ALLOW_OBSERVER_IND),0) <> nvl(trim(S.ALLOW_OBSERVER_IND),0) or
	nvl(trim(T.IS_GROUP_CONTENT),0) <> nvl(trim(S.IS_GROUP_CONTENT),0) or 
    nvl(trim(T.TITLE),0) <> nvl(trim(S.TITLE),0) or 
    nvl(trim(T.PARENT_PK1),0) <> nvl(trim(S.PARENT_PK1),0) or 
    nvl(trim(T.DATA_VERSION),0) <> nvl(trim(S.DATA_VERSION),0) or 
    nvl(trim(T.REVIEWABLE_IND),0) <> nvl(trim(S.REVIEWABLE_IND),0) or 
    nvl(trim(T.VIEW_MODE),0) <> nvl(trim(S.VIEW_MODE),0) or 
    nvl(trim(T.LINK_REF),0) <> nvl(trim(S.LINK_REF),0) or 
    nvl(trim(T.SAMPLE_CONTENT_IND),0) <> nvl(trim(S.SAMPLE_CONTENT_IND),0) or
	nvl(trim(T.PARTIALLY_VISIBLE_IND),0) <> nvl(trim(S.PARTIALLY_VISIBLE_IND),0) or 
    nvl(trim(T.DESCRIPTION),0) <> nvl(trim(S.DESCRIPTION),0) or 
    nvl(trim(T.FOLDER_TYPE),0) <> nvl(trim(S.FOLDER_TYPE),0) or 
    nvl(trim(T.COPY_FROM_PK1),0) <> nvl(trim(S.COPY_FROM_PK1),0)
when not matched then 
insert (
    T.BB_SOURCE,
    T.PK1,
    T.CNTHNDLR_HANDLE,      
    T.DTCREATED,            
    T.DTMODIFIED,           
    T.CONTENT_TYPE,         
    T.POSITION,             
    T.FONT_COLOR,           
    T.TEXT_FORMAT_TYPE,     
    T.OFFLINE_NAME,         
	T.OFFLINE_PATH,         
	T.START_DATE,           
	T.CRSMAIN_PK1,          
	T.END_DATE,             
	T.LESSON_IND,           
	T.SEQUENTIAL_IND,       
	T.NEW_WINDOW_IND,       
	T.TRACKING_IND,         
	T.FOLDER_IND,           
	T.DESCRIBE_IND,         
	T.CARTRIDGE_IND,        
	T.AVAILABLE_IND,        
	T.WEB_URL,              
	T.WEB_URL_HOST,         
	T.ALLOW_GUEST_IND,      
	T.ALLOW_OBSERVER_IND,   
	T.IS_GROUP_CONTENT,     
	T.TITLE,                
	T.PARENT_PK1,           
	T.DATA_VERSION,         
	T.REVIEWABLE_IND,       
	T.VIEW_MODE,            
	T.LINK_REF,             
	T.SAMPLE_CONTENT_IND,   
	T.PARTIALLY_VISIBLE_IND,
	T.DESCRIPTION,          
	T.FOLDER_TYPE,          
	T.COPY_FROM_PK1,        
	T.DELETE_FLAG,          
	T.INSERT_TIME,          
	T.UPDATE_TIME    
) 
values (
	S.BB_SOURCE,
	S.PK1,
	S.CNTHNDLR_HANDLE,      
	S.DTCREATED,            
	S.DTMODIFIED,           
	S.CONTENT_TYPE,         
	S.POSITION,             
	S.FONT_COLOR,           
	S.TEXT_FORMAT_TYPE,     
	S.OFFLINE_NAME,         
	S.OFFLINE_PATH,         
	S.START_DATE,           
	S.CRSMAIN_PK1,          
	S.END_DATE,             
	S.LESSON_IND,           
	S.SEQUENTIAL_IND,       
	S.NEW_WINDOW_IND,       
	S.TRACKING_IND,         
	S.FOLDER_IND,           
	S.DESCRIBE_IND,         
	S.CARTRIDGE_IND,        
	S.AVAILABLE_IND,        
	S.WEB_URL,              
	S.WEB_URL_HOST,         
	S.ALLOW_GUEST_IND,      
	S.ALLOW_OBSERVER_IND,   
	S.IS_GROUP_CONTENT,     
	S.TITLE,                
	S.PARENT_PK1,           
	S.DATA_VERSION,         
	S.REVIEWABLE_IND,       
	S.VIEW_MODE,            
	S.LINK_REF,             
	S.SAMPLE_CONTENT_IND,   
	S.PARTIALLY_VISIBLE_IND,
	S.DESCRIPTION,          
	S.FOLDER_TYPE,          
	S.COPY_FROM_PK1,        
	'N',          
	SYSDATE,          
	SYSDATE)
	;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of BB_COURSE_CONTENTS_S2 rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'BB_COURSE_CONTENTS_S2',
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

END BB_COURSE_CONTENTS_S2_P;
/

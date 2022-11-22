DROP PROCEDURE CSMRT_OWNER.UM_D_XLATITEM_P
/

--
-- UM_D_XLATITEM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_XLATITEM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_D_XLATITEM from PeopleSoft table PSXLATITEM.
--
 --V01  SMT-xxxx 11/21/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_XLATITEM';
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



strMessage01    := 'Merging data into CSMRT_OWNER.UM_D_XLATITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.UM_D_XLATITEM';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_XLATITEM T                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
using (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
),
       S as (
select FIELDNAME, FIELDVALUE, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN 
  from X 
 where X_ORDER = 1)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
select S.FIELDNAME,  	   
       S.FIELDVALUE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       decode(D.XLATLONGNAME, S.XLATLONGNAME, D.XLATLONGNAME, S.XLATLONGNAME) XLATLONGNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(D.XLATSHORTNAME, S.XLATSHORTNAME, D.XLATSHORTNAME, S.XLATSHORTNAME) XLATSHORTNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
  from S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
  left outer join CSMRT_OWNER.UM_D_XLATITEM D                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
    on D.FIELDNAME = S.FIELDNAME                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
   and D.FIELDVALUE = S.FIELDVALUE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
) S                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    on  (T.FIELDNAME = S.FIELDNAME
   and  T.FIELDVALUE = S.FIELDVALUE	
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
 when matched then update set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       T.XLATLONGNAME = S.XLATLONGNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       T.XLATSHORTNAME = S.XLATSHORTNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
 where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(T.XLATLONGNAME,S.XLATLONGNAME,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       decode(T.XLATSHORTNAME,S.XLATSHORTNAME,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  when not matched then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
insert (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.FIELDNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       T.FIELDVALUE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.XLATLONGNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       T.XLATSHORTNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       T.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
values (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.FIELDNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       S.FIELDVALUE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.XLATLONGNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       S.XLATSHORTNAME,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       SYSDATE)
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_XLATITEM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_XLATITEM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.UM_D_XLATITEM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.UM_D_XLATITEM';
update CSMRT_OWNER.UM_D_XLATITEM T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where T.FIELDNAME = S.FIELDNAME
                      and T.FIELDVALUE = S.FIELDVALUE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')					  
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_XLATITEM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_XLATITEM',
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

END UM_D_XLATITEM_P;
/

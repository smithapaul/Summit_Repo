CREATE OR REPLACE PROCEDURE CREATE_SID_BITMAPS_P IS
   bitmap_cur            SYS_REFCURSOR;
   v_sql_str             varchar2(2000);
   v_bitmap_total        number;

BEGIN

-- Open cursor for bitmap indexes.

   v_bitmap_total := 0;

   OPEN bitmap_cur FOR
select 'create bitmap index AK_'||substr(TABLE_NAME,1,24)||COLUMN_ID||' on '||TABLE_NAME||'('||COLUMN_NAME||') nologging tablespace '||decode(substr(TABLE_NAME,-2,2),'MV','CSMV_INDEX1','CSMRT_INDEX1')||' parallel (degree 8)'
from USER_TAB_COLUMNS T
where TABLE_NAME in (select TABLE_NAME from USER_TABLES)
  and TABLE_NAME not like 'PS_X%'
  and substr(TABLE_NAME,1,2) in ('PS','UM')
  and TABLE_NAME not like '%NEW%'
  and TABLE_NAME not like '%OLD%'
  and TABLE_NAME not like '%EXEC%'
  and substr(TABLE_NAME,-1,1) not between '0' and '9'
--  and substr(TABLE_NAME,3,3) not in ('_D_','_H_')
--  and TABLE_NAME not in (select TABLE_NAME from USER_TABLES where TABLE_NAME not like 'UM%' 
--                         intersect select 'PS'||substr(TABLE_NAME,3,30) from USER_TABLES where TABLE_NAME like 'UM%' 
--                         union select replace(TABLE_NAME,'_AGG','') from USER_TABLES where TABLE_NAME like '%AGG'
--                         union select TABLE_NAME from USER_TABLES where NUM_ROWS < 10000)
  and not exists (select 1 from USER_IND_COLUMNS I where I.TABLE_NAME = T.TABLE_NAME and I.COLUMN_NAME = T.COLUMN_NAME)
  and exists (select 1 from USER_CONSTRAINTS CT, USER_CONS_COLUMNS CC 
                       where CT.CONSTRAINT_NAME = CC.CONSTRAINT_NAME 
                         and CONSTRAINT_TYPE = 'R' 
                         and substr(T.TABLE_NAME,3,3) in ('_D_','_F_','_R_') 
                         and CT.TABLE_NAME = T.TABLE_NAME
                         and CC.COLUMN_NAME = T.COLUMN_NAME)
  and (COLUMN_NAME like '%_SID' or COLUMN_NAME like '%_ORDER')  -- Modified 5/25/2012 
  and COLUMN_NAME <> 'BATCH_SID'
  and substr(COLUMN_NAME,-6,6) <> 'DT_SID'
order by TABLE_NAME, COLUMN_ID;

   LOOP
      FETCH bitmap_cur INTO v_sql_str;

      EXIT WHEN bitmap_cur%NOTFOUND;

      EXECUTE IMMEDIATE v_sql_str;

         v_bitmap_total := v_bitmap_total + 1;

         --DBMS_OUTPUT.PUT_LINE('SQL is: '||v_sql_str);

   END LOOP;

         DBMS_OUTPUT.PUT_LINE('Total SID BITMAP indexes created: '||v_bitmap_total);
         
      CLOSE bitmap_cur;
      
END CREATE_SID_BITMAPS_P;
/

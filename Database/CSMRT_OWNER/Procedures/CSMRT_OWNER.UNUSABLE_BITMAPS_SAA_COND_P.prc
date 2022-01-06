CREATE OR REPLACE PROCEDURE             UNUSABLE_BITMAPS_SAA_COND_P IS
   bitmap_cur            SYS_REFCURSOR;
   v_sql_str             varchar2(2000);
   v_bitmap_total        number;

BEGIN

-- Open cursor for bitmap indexes.

   v_bitmap_total := 0;

   OPEN bitmap_cur FOR
    select 'alter index '||INDEX_NAME||' unusable'
      from USER_IND_COLUMNS
     where INDEX_NAME in (select INDEX_NAME from USER_INDEXES where INDEX_TYPE = 'BITMAP' and TABLE_NAME = 'UM_F_SAA_ADB_COND')
       and INDEX_NAME not like 'PK%'
     order by TABLE_NAME, COLUMN_NAME;

   LOOP
      FETCH bitmap_cur INTO v_sql_str;

      EXIT WHEN bitmap_cur%NOTFOUND;

      EXECUTE IMMEDIATE v_sql_str;

         v_bitmap_total := v_bitmap_total + 1;

         --DBMS_OUTPUT.PUT_LINE('SQL is: '||v_sql_str);

   END LOOP;

         DBMS_OUTPUT.PUT_LINE('Total SID BITMAP indexes altered: '||v_bitmap_total);
         
      CLOSE bitmap_cur;
      
END UNUSABLE_BITMAPS_SAA_COND_P;
/

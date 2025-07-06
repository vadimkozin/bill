   """
   UPDATE `test`
   SET
   `price` =  CASE
       WHEN `id` = 1 THEN 11
       WHEN `id` = 2 THEN 22
   ELSE `price` END,
   `title` = CASE
       WHEN `id` = 1 THEN 'title 1'
       WHEN `id` = 2 THEN 'title 2'
       WHEN `id` = 3 THEN 'title 3'
   ELSE `title` END
   WHERE `id` IN (1,2,3)
   """
   """
   UPDATE {table} SET `fm3`=CASE
   WHEN `id`=1 THEN '6261234'
   WHEN `id`=2 THEN '6424567'
   ELSE `fm3` END
   WHERE `id` IN (1,2,3)
   """
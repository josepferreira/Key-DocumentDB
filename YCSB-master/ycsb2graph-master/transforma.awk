#! /usr/bin/awk -f
BEGIN {  }
/OVERALL/      { print}
/TOTAL_GCS_PS_Scavenge/      { print}
/TOTAL_GC_TIME_PS_Scavenge/      { print}
/TOTAL_GC_TIME_%_PS_Scavenge/      { print}
/TOTAL_GCS_PS_MarkSweep/ { print}
/TOTAL_GC_TIME_PS_MarkSweep/ { print}
/TOTAL_GC_TIME_%_PS_MarkSweep/ { print}
/TOTAL_GCs/ { print}
/TOTAL_GC_TIME/ { print}
/READ/ { print}
/CLEANUP/ { print}
/UPDATE/ { print}
/READ-MODIFY-WRITE/ {print}
/INSERT/ {print}
/SCAN/ {print}
END   {   }
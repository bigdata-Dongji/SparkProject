package taxi
/*
样例类，用来存储所需数据的对象
 */
case class Trip(
               license:String,//出租车执照证
               pickUpTime:Long,//上车时间
               dropOffTime:Long,//下车时间
               pickUpX:Double,//上车地点经度
               pickUpY:Double,//上车地点维度
               dropOffX:Double,//下车地点经度
               dropOffY:Double//下车地点维度
               )

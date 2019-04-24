'''
Author: Hieu Phung
Date created: 2019-01-07
Python Version: 3.6
'''

import json
import os
import sys
import psycopg2
from datetime import datetime, timedelta
from statistics import mean, stdev

dev1_conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"


def myMain():
    
    # if len(sys.argv) != 2:
    #     print("Usage: python3 traceJobChain.py <prJobId>")
    #     sys.exit()
    
    conn = psycopg2.connect(dev1_conn_string)
    cur = conn.cursor()
    
    # prJobId = int(sys.argv[1])
    
    print("\n")
    
    # fileNodes = {}
    # jobNodes = {}
    groups = []
    items = []

    getGroup1(cur, groups, items)
    
    # print(groups)
    print(items)
    
    cur.close()
    conn.close()



def getGroup1(cur, groups, items):
    
    # productIds = [3263, 3266, 3269, 3272, 3273, 3277, 3279, 3282, 3290, 3291, 3292, 3294, 3295, 3298, 3302, 3305, 3311, 3321, 3323, 3325, 3326, 3330, 3331, 3333, 3334, 3335, 3336, 3338, 3342, 3346, 3347, 3351, 3358, 3360, 3364, 3365, 3367, 3368, 3371, 3372, 3373, 3374, 4251, 4257, 4263, 4265, 4267, 4268, 4270, 4273, 4276, 4277, 4279, 4283, 4284, 4286, 4287, 4288, 4292, 4293, 4295, 4297, 4298, 4304, 4307, 4308, 4310, 4313, 4315, 4319, 4873, 4874, 4875, 4876, 4877, 4878, 4879, 4880, 4881, 4882, 4883, 4884, 4885, 4886, 4887, 4888, 4889, 4890, 4891, 4892, 4893, 4894, 4895, 4896, 4897, 4898, 4899, 4900, 4901, 4902, 4903, 4904, 4909, 4910, 4911, 4912, 4917, 4918, 4919, 4920, 4925, 4926, 4927, 4928, 4933, 4934, 4935, 4936, 4945, 4946, 4947, 4948, 4949, 4950, 4951, 4952, 4955, 4956, 5021, 5022, 5023, 5024, 5025, 5026, 5027, 5028, 5029, 5030, 5031, 5032, 5033, 5034, 5035, 5036, 5037, 5038, 5039, 5040, 5041, 5042, 5043, 5044, 5045, 5046, 5047, 5048, 5049, 5050, 5051, 5052, 5053, 5054, 5055, 5056, 5057, 5058, 5059, 5060, 5061, 5062, 5063, 5064, 5065, 5066, 5067, 5068, 5069, 5070, 5071, 5072, 5073, 5074, 5075, 5076, 5077, 5078, 5079, 5080, 5081, 5082, 5083, 5084, 5089, 5090, 5091, 5092, 5097, 5098, 5099, 5100, 5105, 5106, 5107, 5108, 5113, 5114, 5115, 5116, 5121, 5122, 5123, 5124, 5129, 5130, 5131, 5132, 5135, 5136]
    productIds = [3605, 3606, 3607, 3608, 3634, 3638, 3643, 3647, 3648, 3759, 3760, 3761, 3762, 3763, 3764, 3765, 3766, 3767, 3768, 3769, 3770, 3771, 3772, 3773, 3774, 3775, 3776, 3777, 3778, 3779, 3780, 3781, 3782, 3783, 3784, 3785, 3786, 3787, 3788, 3789, 3790, 3791, 3792, 3793, 3794, 3795, 3796, 3797, 3798, 3799, 3800, 3801, 3802, 3803, 3804, 3805, 3806, 3807, 3808, 3809, 3810, 3811, 3812, 3813, 3814, 3815, 3816, 3817, 3818, 3819, 3820, 3821, 3822, 3823, 3824, 3825, 3826, 3827, 3828, 3829, 3830, 3831, 3832, 3833, 3834, 3835, 3836, 3837, 3838, 3839, 3840, 3841, 3842, 3843, 3844, 3860, 3861, 3863, 3865, 3866, 3867, 3868, 3869, 3870, 3871, 3872, 3876, 3877, 3878, 3879, 3881, 3883, 3884, 3885, 3887, 3888, 3889, 3891, 3892, 3894, 3895, 3897, 3898, 3900, 3901, 3903, 3905, 3906, 3908, 3909, 3913, 3914, 3915, 3917, 3919, 3920, 3921, 3923, 3925, 3926, 3927, 3929, 3931, 3932, 3933, 3934, 3936, 3937, 3938, 3940, 3941, 3942, 3944, 3945, 3946, 3947, 3948, 3950, 3951, 3953, 3954, 3955, 3956, 3957, 3958, 3959, 3962, 3964, 3965, 3966, 3969, 3970, 3971, 3974, 3975, 3977, 3978, 3979, 3980, 3981, 3982, 3983, 3984, 3987, 3988, 3989, 3990, 3993, 3994, 3996, 3997, 3999, 4000, 4001, 4002, 4004, 4006, 4007, 4008, 4009, 4010, 4011, 4012, 4013, 4014, 4015, 4016, 4018, 4019, 4021, 4022, 4023, 4024, 4026, 4027, 4028, 4029, 4030, 4031, 4032, 4033, 4034, 4036, 4037, 4038, 4039, 4042, 4045, 4046, 4051, 4055, 4056, 4057, 4058, 4060, 4065, 4066, 4067, 4068, 4069, 4070, 4071, 4072, 4075, 4077, 4078, 4079, 4080, 4081, 4082, 4084, 4085, 4086, 4089, 4092, 4094, 4095, 4096, 4098, 4100, 4101, 4102, 4106, 4107, 4108, 4110, 4111, 4113, 4115, 4116, 4117, 4120, 4122, 4124, 4125, 4126, 4127, 4128, 4129, 4133, 4135, 4137, 4138, 4139, 4140, 4141, 4142, 4143, 4144, 4145, 4151, 4152, 4153, 4154, 4155, 4157, 4160, 4161, 4162, 4163, 4164, 4165, 4166, 4167, 4169, 4171, 4172, 4173, 4175, 4176, 4177, 4179, 4181, 4182, 4183, 4185, 4186, 4187, 4188, 4189, 4190, 4191, 4193, 4194, 4195, 4196, 4198, 4199, 4200, 4201, 4202, 4203, 4204, 4205, 4207, 4208, 4209, 4210, 4211, 4212, 4213, 4214, 4215, 4216, 4219, 4220, 4221, 4222, 4223, 4224, 4225, 4226, 4227, 4229, 4230, 4418, 4422, 4423, 4428, 4430, 4431, 4457, 4459, 4630, 4631, 4633, 4635, 4637, 4640, 4641, 4643, 4646, 4647, 4648, 4649, 4651, 4654, 4656, 4657, 4661, 4662, 4666, 4668, 4669, 4670, 4671, 4672, 4674, 4678, 4680, 4681, 4683, 4684, 4685, 4687, 4690, 4691, 4692, 4693, 4694, 4696, 4697, 4699, 4700, 4701, 4702, 4703, 4706, 4708, 4711, 4713, 4714, 4715, 4717, 4718, 4719, 4720, 4721, 4726, 4732, 4733, 4734, 4736, 4742, 4744, 4745, 4747, 4748, 4750, 4751, 4752, 4755, 4756, 4757, 4761, 4763, 4764, 4765, 4766, 4768, 4769, 4773, 4774, 4775, 4776, 4778, 4780, 4789, 4790, 4792, 4795, 4797, 4798, 4802, 4803, 4804, 4807, 4808, 4809, 4811, 4816, 4818, 4820, 4821, 4824, 4825, 4826, 4828, 4830, 4835, 4837, 4838]

    cur.execute("""SELECT
         productid,
         productshortname
       FROM productDescription 
       WHERE productid in %s
       """, (tuple(productIds),) )
    rows = cur.fetchall()
    print(len(rows))
    
    for row in rows:
        # print(row)
        pi, psn = row

        groupItem = {
            'id': pi,
            'content': psn,
        }
        groups.append(groupItem)
    
    print("Getting latest files")
    cur.execute("""SELECT
         fileid,
         filename,
         fileinserttime,
         filestarttime,
         fileendtime,
         productid
       FROM filemetadata 
       WHERE fileendtime > now() - interval '45' minute
         AND productid in %s
       """, (tuple(productIds),) )
    rows = cur.fetchall()
    print(len(rows))
    
    for row in rows:
        # print(row)
        fi, fn, fit, fst, fet, pi = row
        
        fileItem = {
            'id': fi,
            'content': str(fi),
            'group': pi,
            # 'title': "Filename: %s<br/>InsertTime: %s" % (fn, str(fit)),
            'start': fst.strftime('%Y-%m-%d %H:%M:%S'),
            'end': fet.strftime('%Y-%m-%d %H:%M:%S'),
        }
        
        items.append(fileItem)
        
        # print(jobNode)
        # print(fileNode)
        # print(edge)
        


if __name__ == "__main__":
    myMain()

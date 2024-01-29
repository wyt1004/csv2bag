import csv
import os
import numpy as np
import rospy
from sensor_msgs.msg import PointField
import sensor_msgs.msg
from std_msgs.msg import Header
import sensor_msgs.point_cloud2 as pc2
import rosbag
import argparse

def parse_opt(known=False):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str, default='', help='lidar file path')
    parser.add_argument('--out_path', type=str, default='./output.bag', help='bag out path')
    return parser.parse_known_args()[0] if known else parser.parse_args()

def save_lidar_data(input_path, out_path, topic, frame_id='map'):
    print("start transform...")
    bag = rosbag.Bag(out_path, "w")
    datetimes = []
    with open(os.path.join(input_path, 'Laser_Timestamp_Start.txt')) as f:
        lines = f.readlines()
        for line in lines:
            datetimes.append(float(line))
    csv_file_path = os.path.join(input_path, "Lidar")
    for i in range(len(datetimes)):
        csv_file = os.path.join(csv_file_path, str(i).zfill(10)+".csv")
        pc_data = []
        
        # Read CSV file
        with open(csv_file, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                x, y, z, _, _, _, intensity, _ = map(float, row)
                pc_data.extend([x, y, z, intensity])
        scan = np.array(pc_data, dtype=np.float32).reshape(-1,4)

        # create header
        header = Header()
        header.frame_id = frame_id
        header.stamp = rospy.Time.from_sec(datetimes[i])
        
        # fill pcl msg
        fields = [PointField('x', 0, PointField.FLOAT32, 1),
                  PointField('y', 4, PointField.FLOAT32, 1),
                  PointField('z', 8, PointField.FLOAT32, 1),
                  PointField('intensity', 12, PointField.FLOAT32, 1)]
        
        pcl_msg = pc2.create_cloud(header, fields, scan)

        bag.write(topic, pcl_msg, t=pcl_msg.header.stamp)
        print(csv_file + " done!!!!!")

    print("## OVERVIEW ##")
    print(bag)

    # Close the ROS Bag
    bag.close()

def main(opt):
    # file_path = '/home/pc/project/data/Wate_SLAM/H05_7_Sequence_160_270/Lidar/'
    velo_topic = '/velodyne_points'
    save_lidar_data(opt.input_path, opt.out_path, velo_topic)

if __name__ == '__main__':
    opt = parse_opt()
    main(opt)

    # run script --- python lidar2bag.py --input_path input_path --out_path output_bag_path.bag
    # eg. python lidar2bag.py --input_path /home/pc/project/data/Wate_SLAM/H05_7_Sequence_160_270/Lidar/ --out_path ./output.bag
    


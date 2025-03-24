from naolib_producer import main as naolib_main
from plane_producer import main as plane_main


def main(selected_coordinates):
    plane_main()
    naolib_main(selected_coordinates)

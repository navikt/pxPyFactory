from .main import PXMain

def go():
    print('=' * 80) # print separator line
    px_main = PXMain()
    px_main.run()
    print('')

if __name__ == "__main__":
    go()

from .main import PXMain

def go():
    print('-- start pxPyFactory --')
    px_main = PXMain()
    px_main.run()
    print('-- stop  pxPyFactory --')

if __name__ == "__main__":
    go()

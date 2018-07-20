from .app import app_factory

my_app = app_factory()


if __name__ == '__main__':
    my_app.run(debug=True)

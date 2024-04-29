from .request import Request

class Batch1:
    def send_request(self, message):
        request = Request()
        print('sendの前')
        return_message = request.send(message)
        print('sendの後')
        return return_message
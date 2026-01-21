from abc import ABC, abstractmethod


class AuthService(ABC):

    def __init__(self):
        self.clients = {}

    @abstractmethod
    async def validate_token(self, token) -> dict:
        """
        Validates an auth token asynchronously.
        
        Returns an object which includes user id on success,
        or an error object with a specific error message on failure.
        """
        pass

    @abstractmethod
    def validate_user_order(self, user_id, order_amount, side):
        pass

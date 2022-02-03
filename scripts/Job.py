class Job:
    def __init__(self,
        type: str,
        label: str,
        kwargs: dict,
        force_run: bool
    ) -> None:
        self.type = type
        self.label = label
        self.kwargs = kwargs
        self.force_run = force_run
    def to_dict(self):
        return {
            'type': self.type,
            'label': self.label,
            'kwargs': self.kwargs,
            'force_run': self.force_run
        }
    def key(self):
        return {
            'type': self.type,
            'kwargs': self.kwargs
        }
    @staticmethod
    def from_dict(x: dict):
        return Job(
            type=x['type'],
            label=x['label'],
            kwargs=x['kwargs'],
            force_run=x['force_run']
        )
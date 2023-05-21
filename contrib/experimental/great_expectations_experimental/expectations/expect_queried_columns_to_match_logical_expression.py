# pylint: disable = W0613
"""
This is a template for creating custom QueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Any, List, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult, QueryExpectation, render_evaluation_parameter_string
)
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (RenderedStringTemplateContent,
                                             RenderedTableContent,
                                             RenderedBulletListContent,
                                             RenderedGraphContent)
from great_expectations.render.util import substitute_none_for_missing


# This class defines the Expectation itself
class ExpectQueriedColumnsToMatchLogicalExpression(QueryExpectation):
    """Expect the table's columns values to match the given logical expression"""

    # This is the id string of the Metric(s) used by this Expectation.
    metric_dependencies = ('query.template_values',)

    # This is the default, baked-in SQL Query for this QueryExpectation

    query = """ SELECT COUNT(*)
                    FROM (SELECT {column_list}
                    FROM {active_batch} a
                    WHERE not ({expression})) b
                """

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ('query', 'expression', 'template_dict')

    domain_keys = ('batch_id', 'row_condition', 'condition_parser')

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        'result_format': 'BASIC',
        'include_config': True,
        'catch_exceptions': False,
        'meta': None,
        'template_dict': None,
        'expression': None,
        'query': query,
    }

    def validate_configuration(
            self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        column_list = configuration['kwargs'].get('template_dict')['column_list']
        expression = configuration['kwargs'].get('template_dict')['expression']

        try:
            assert column_list is not None, 'column list must be specified'
            assert expression is not None, 'logical expression must be specified'
            assert (isinstance(column_list, str) and len(column_list) > 0), 'column list must be string'
            assert (isinstance(expression, str) and len(expression) > 0), 'logical expression must be a string'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
            self,
            configuration: ExpectationConfiguration,
            metrics: dict,
            runtime_configuration: dict = None,
            execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        query_count_result = list(metrics.get('query.template_values')[0].values())[0]
        if query_count_result == 0:
            return {
                'success': True,
                'result': {'info': 'The table matches the logical expression',
                           'observed_value': 'matches logical expression'}
            }
        return {
            'success': False,
            'result': {
                'info': 'The table does not match the logical expression',
                'observed_value': f'bad records count: {query_count_result}',
            },
        }

    @classmethod
    @renderer(renderer_type='renderer.prescriptive')
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration: ExpectationConfiguration = None,
            result: ExpectationValidationResult = None,
            language: str = None,  # pylint disable: W0613
            runtime_configuration: dict = None,
            **kwargs,
    ) -> List[Union[dict, str, RenderedStringTemplateContent, RenderedTableContent,
                    RenderedBulletListContent, RenderedGraphContent, Any]]:

        runtime_configuration = runtime_configuration or {}

        styling = runtime_configuration.get('styling')

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                'template_dict',
            ],
        )

        # build string template
        template_str = f'expect table to match the expression {params.get("template_dict")["expression"]} '

        # return simple string
        return [
            RenderedStringTemplateContent(
                **{
                    'content_block_type': 'string_template',
                    'string_template': {
                        'template': template_str,
                        'params': params,
                        'styling': styling,
                    },
                }
            )
        ]

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            'data': [
                {
                    'data': {'col1': [1, 2, 3, 4, 5, 5], 'col2': [10, 3, 4, 4, 5, 5], 'col3':[1, 2, 2, 3, 4, 4]},
                }
            ],
            'only_for': ['sqlite', 'vertica', 'mysql', 'postgresql', 'spark'],
            'tests': [
                {
                    'title': 'basic_positive_test_and',
                    'exact_match_out': False,
                    'include_in_gallery': True,
                    'in': {
                        'template_dict': {'column_list': 'col1,col2,col3',
                                          'expression': 'col1>0 and col2>=3 and col3<5'},
                    },
                    'out': {'success': True},
                },
                {
                    'title': 'basic_negative_test_and',
                    'exact_match_out': False,
                    'include_in_gallery': True,
                    'in': {
                        'template_dict': {'column_list': 'col1,col2',
                                          'expression': 'col1>1 and col2>=3'},
                    },
                    'out': {'success': False},
                },
                {
                    'title': 'basic_positive_test_or',
                    'exact_match_out': False,
                    'include_in_gallery': True,
                    'in': {
                        'template_dict': {'column_list': 'col1,col3',
                                          'expression': 'col1>3 or col3<3'},
                    },
                    'out': {'success': True},
                },
                {
                    'title': 'basic_negative_test_or',
                    'exact_match_out': False,
                    'include_in_gallery': True,
                    'in': {
                        'template_dict': {'column_list': 'col1,col2',
                                          'expression': 'col1=5 or col2=4'},
                    },
                    'out': {'success': False},
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        'tags': ['query-based'],  # Tags for this Expectation in the Gallery
        'contributors': [  # Github handles for all contributors to this Expectation.
            '@marcuselad',  # Don't forget to add your github handle here!
        ],
    }


if __name__ == '__main__':
    ExpectQueriedColumnsToMatchLogicalExpression().print_diagnostic_checklist()

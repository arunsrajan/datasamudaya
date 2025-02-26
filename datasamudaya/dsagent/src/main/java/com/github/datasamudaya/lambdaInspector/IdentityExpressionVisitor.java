package com.github.datasamudaya.lambdaInspector;

import com.github.datasamudaya.lambdaInspector.expr.ArgumentExpression;
import com.github.datasamudaya.lambdaInspector.expr.ArrayLengthExpression;
import com.github.datasamudaya.lambdaInspector.expr.ArrayLoadExpression;
import com.github.datasamudaya.lambdaInspector.expr.BinaryArithmeticExpression;
import com.github.datasamudaya.lambdaInspector.expr.CapturedArgExpression;
import com.github.datasamudaya.lambdaInspector.expr.CastExpression;
import com.github.datasamudaya.lambdaInspector.expr.ConstExpression;
import com.github.datasamudaya.lambdaInspector.expr.Expression;
import com.github.datasamudaya.lambdaInspector.expr.ExpressionVisitor;
import com.github.datasamudaya.lambdaInspector.expr.GetFieldExpression;
import com.github.datasamudaya.lambdaInspector.expr.InstanceOfExpression;
import com.github.datasamudaya.lambdaInspector.expr.MethodInvocationExpression;
import com.github.datasamudaya.lambdaInspector.expr.NewArrayExpression;
import com.github.datasamudaya.lambdaInspector.expr.NewExpression;
import com.github.datasamudaya.lambdaInspector.expr.ReturnAddressExpression;
import com.github.datasamudaya.lambdaInspector.expr.ThisExpression;
import com.github.datasamudaya.lambdaInspector.expr.UnaryExpression;
import com.github.datasamudaya.lambdaInspector.expr.UnknownExpression;

public class IdentityExpressionVisitor implements ExpressionVisitor<Expression> {

	@Override
	public Expression visit(MethodInvocationExpression expr) {
		return expr;
	}

	@Override
	public Expression visit(ConstExpression constExpression) {
		return constExpression;
	}

	@Override
	public Expression visit(ReturnAddressExpression returnAddressExpression) {
		return returnAddressExpression;
	}

	@Override
	public Expression visit(GetFieldExpression getFieldExpression) {
		return getFieldExpression;
	}

	@Override
	public Expression visit(NewExpression newExpression) {
		return newExpression;
	}

	@Override
	public Expression visit(ArgumentExpression argumentExpression) {
		return argumentExpression;
	}

	@Override
	public Expression visit(CapturedArgExpression capturedArgExpression) {
		return capturedArgExpression;
	}

	@Override
	public Expression visit(UnaryExpression unaryExpression) {
		return unaryExpression;
	}

	@Override
	public Expression visit(NewArrayExpression newArrayExpression) {
		return newArrayExpression;
	}

	@Override
	public Expression visit(ArrayLengthExpression arrayLengthExpression) {
		return arrayLengthExpression;
	}

	@Override
	public Expression visit(CastExpression castExpression) {
		return castExpression;
	}

	@Override
	public Expression visit(InstanceOfExpression instanceOfExpression) {
		return instanceOfExpression;
	}

	@Override
	public Expression visit(ThisExpression thisExpression) {
		return thisExpression;
	}

	@Override
	public Expression visit(ArrayLoadExpression arrayLoadExpression) {
		return arrayLoadExpression;
	}

	@Override
	public Expression visit(BinaryArithmeticExpression binaryArithmeticExpression) {
		return binaryArithmeticExpression;
	}

	@Override
	public Expression visit(UnknownExpression unknownExpression) {
		return unknownExpression;
	}

}
